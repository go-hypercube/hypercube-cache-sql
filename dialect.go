package sqlcache

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Dialect identifies a supported database/sql backend.
type Dialect string

const (
	// PostgreSQL selects PostgreSQL 9.5 or newer.
	PostgreSQL Dialect = "postgres"
	// MySQL selects MySQL 8.0 or newer with InnoDB.
	MySQL Dialect = "mysql"
	// SQLite selects SQLite 3.35 or newer, for UPDATE RETURNING support.
	SQLite Dialect = "sqlite"
)

var (
	// ErrNilDatabase is returned by New when db is nil.
	ErrNilDatabase = errors.New("sql cache: database is required")

	// ErrUnsupportedDialect is returned by New for an unknown dialect.
	ErrUnsupportedDialect = errors.New("sql cache: unsupported dialect")

	// ErrInvalidInteger means an Increment operand or result is not an int64.
	ErrInvalidInteger = errors.New("sql cache: value is not a signed 64-bit integer")
)

func (d Dialect) bind(query string) string {
	if d != PostgreSQL {
		return query
	}
	var b strings.Builder
	parameter := 1
	for _, char := range query {
		if char == '?' {
			fmt.Fprintf(&b, "$%d", parameter)
			parameter++
		} else {
			b.WriteRune(char)
		}
	}
	return b.String()
}

func (d Dialect) nowMicros() string {
	switch d {
	case PostgreSQL:
		return "CAST(EXTRACT(EPOCH FROM clock_timestamp()) * 1000000 AS BIGINT)"
	case MySQL:
		// UNIX_TIMESTAMP interprets CURRENT_TIMESTAMP in the session's time
		// zone. Using UTC_TIMESTAMP here would apply a non-UTC offset twice.
		return "CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6)) * 1000000 AS SIGNED)"
	default: // SQLite; New rejects unsupported dialects.
		return "CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)"
	}
}

func (d Dialect) migrationStatements() []string {
	switch d {
	case MySQL:
		return []string{`CREATE TABLE IF NOT EXISTS hypercube_cache_entries (
			storage_scope CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
			cache_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
			cache_value LONGBLOB NOT NULL,
			expires_at_us BIGINT NULL,
			PRIMARY KEY (storage_scope, cache_key),
			INDEX hypercube_cache_expiry_idx (storage_scope, expires_at_us)
		) ENGINE=InnoDB`}
	default:
		valueType := "BLOB"
		if d == PostgreSQL {
			valueType = "BYTEA"
		}
		return []string{
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS hypercube_cache_entries (
				storage_scope VARCHAR(64) NOT NULL,
				cache_key VARCHAR(64) NOT NULL,
				cache_value %s NOT NULL,
				expires_at_us BIGINT NULL,
				PRIMARY KEY (storage_scope, cache_key)
			)`, valueType),
			`CREATE INDEX IF NOT EXISTS hypercube_cache_expiry_idx
				ON hypercube_cache_entries (storage_scope, expires_at_us)`,
		}
	}
}

// Migrate creates the cache table and expiry index. It is idempotent and should
// run during deployment or startup before cache operations begin.
func (c *Cache) Migrate(ctx context.Context) error {
	for _, statement := range c.dialect.migrationStatements() {
		if _, err := c.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("sql cache: migrate: %w", err)
		}
	}
	return nil
}

func (d Dialect) upsert(overwrite bool, expiration string) string {
	query := `INSERT INTO hypercube_cache_entries
		(storage_scope, cache_key, cache_value, expires_at_us) VALUES (?, ?, ?, ` + expiration + `)`
	if d == MySQL {
		if overwrite {
			// Repeated parameters avoid MySQL's deprecated VALUES() function.
			return query + ` ON DUPLICATE KEY UPDATE cache_value = ?, expires_at_us = ` + expiration
		}
		return query + ` ON DUPLICATE KEY UPDATE cache_key = cache_key`
	}
	if overwrite {
		return query + ` ON CONFLICT (storage_scope, cache_key) DO UPDATE
			SET cache_value = excluded.cache_value, expires_at_us = excluded.expires_at_us`
	}
	// A no-op UPDATE, rather than DO NOTHING, acquires the conflicting row's
	// write lock before Increment reads it. SQLite acquires its writer lock too.
	return query + ` ON CONFLICT (storage_scope, cache_key) DO UPDATE SET cache_key = excluded.cache_key`
}
