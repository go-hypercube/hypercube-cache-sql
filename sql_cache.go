// Package sqlcache implements Hypercube's cache.Cache using database/sql.
// PostgreSQL, MySQL (InnoDB), and SQLite are supported. SQL drivers are supplied
// by the application; this package does not register a driver or own the DB.
package sqlcache

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/go-hypercube/go-hypercube/cache"
)

// DefaultStorageNamespace is the shared scope used when no namespace is set.
const DefaultStorageNamespace = "hypercube:cache"

// Option configures a Cache at construction time.
type Option func(*Cache)

// WithStorageNamespace isolates keys and cleanup within a shared cache table.
// Instances with the same database and namespace share their cached entries.
func WithStorageNamespace(namespace string) Option {
	return func(c *Cache) { c.storageScope = hashKey(namespace) }
}

// Cache is a persistent cache backed by a caller-owned *sql.DB. It is safe for
// concurrent use, including across processes. Use New and Migrate before use.
type Cache struct {
	db           *sql.DB
	dialect      Dialect
	storageScope string
}

var _ cache.Cache = (*Cache)(nil)

// New validates configuration without contacting the database. It does not
// migrate the schema, start goroutines, or take ownership of db.
func New(db *sql.DB, dialect Dialect, options ...Option) (*Cache, error) {
	if db == nil {
		return nil, ErrNilDatabase
	}
	switch dialect {
	case PostgreSQL, MySQL, SQLite:
	default:
		return nil, fmt.Errorf("%w %q", ErrUnsupportedDialect, dialect)
	}
	c := &Cache{db: db, dialect: dialect, storageScope: hashKey(DefaultStorageNamespace)}
	for _, option := range options {
		if option != nil {
			option(c)
		}
	}
	return c, nil
}

// Get returns ErrNotFound for missing or expired keys.
func (c *Cache) Get(ctx context.Context, key string) (string, error) {
	query := `SELECT cache_value FROM hypercube_cache_entries
		WHERE storage_scope = ? AND cache_key = ?
		AND (expires_at_us IS NULL OR expires_at_us > ` + c.dialect.nowMicros() + `)`
	var value []byte
	err := c.db.QueryRowContext(ctx, c.dialect.bind(query), c.storageScope, hashKey(key)).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", cache.ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("sql cache: get: %w", err)
	}
	return string(value), nil
}

// Set overwrites a value and its TTL. Zero TTL means no expiration.
func (c *Cache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if ttl < 0 {
		return cache.ErrInvalidTTL
	}
	// Compute expiration on the server in the write statement, avoiding clock
	// skew between application instances. NULL represents no expiration.
	expiration := c.dialect.nowMicros() + ` + ?`
	query := c.dialect.upsert(true, expiration)
	args := []any{c.storageScope, hashKey(key), []byte(value), ttlArgument(ttl)}
	if c.dialect == MySQL {
		args = append(args, []byte(value), ttlArgument(ttl))
	}
	if _, err := c.db.ExecContext(ctx, c.dialect.bind(query), args...); err != nil {
		return fmt.Errorf("sql cache: set: %w", err)
	}
	return nil
}

// Delete removes a key, including an expired key. Missing keys are harmless.
func (c *Cache) Delete(ctx context.Context, key string) error {
	query := `DELETE FROM hypercube_cache_entries WHERE storage_scope = ? AND cache_key = ?`
	if _, err := c.db.ExecContext(ctx, c.dialect.bind(query), c.storageScope, hashKey(key)); err != nil {
		return fmt.Errorf("sql cache: delete: %w", err)
	}
	return nil
}

// Has reports whether a key exists and is unexpired.
func (c *Cache) Has(ctx context.Context, key string) (bool, error) {
	query := `SELECT 1 FROM hypercube_cache_entries WHERE storage_scope = ? AND cache_key = ?
		AND (expires_at_us IS NULL OR expires_at_us > ` + c.dialect.nowMicros() + `)`
	var found int
	err := c.db.QueryRowContext(ctx, c.dialect.bind(query), c.storageScope, hashKey(key)).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("sql cache: has: %w", err)
	}
	return true, nil
}

// Increment atomically adds delta, creating missing or expired keys from zero.
// Existing live TTLs are preserved. Invalid integers and overflow leave the
// entry unchanged and return ErrInvalidInteger.
func (c *Cache) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("sql cache: begin increment: %w", err)
	}
	defer rollback(tx)
	keyHash := hashKey(key)
	_, err = tx.ExecContext(ctx, c.dialect.bind(c.dialect.upsert(false, "?")),
		c.storageScope, keyHash, []byte("0"), nil)
	if err != nil {
		return 0, fmt.Errorf("sql cache: lock increment: %w", err)
	}

	var value []byte
	var expiresAt sql.NullInt64
	var now int64
	query := `SELECT cache_value, expires_at_us, ` + c.dialect.nowMicros() + `
		FROM hypercube_cache_entries WHERE storage_scope = ? AND cache_key = ?`
	if err := tx.QueryRowContext(ctx, c.dialect.bind(query), c.storageScope, keyHash).
		Scan(&value, &expiresAt, &now); err != nil {
		return 0, fmt.Errorf("sql cache: read increment: %w", err)
	}
	var current int64
	if expiresAt.Valid && expiresAt.Int64 <= now {
		expiresAt = sql.NullInt64{}
	} else {
		current, err = strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%w: %w", ErrInvalidInteger, err)
		}
	}
	if (delta > 0 && current > math.MaxInt64-delta) || (delta < 0 && current < math.MinInt64-delta) {
		return 0, fmt.Errorf("%w: increment overflows int64", ErrInvalidInteger)
	}
	result := current + delta
	query = `UPDATE hypercube_cache_entries SET cache_value = ?, expires_at_us = ?
		WHERE storage_scope = ? AND cache_key = ?`
	if _, err := tx.ExecContext(ctx, c.dialect.bind(query), []byte(strconv.FormatInt(result, 10)),
		expiresAt, c.storageScope, keyHash); err != nil {
		return 0, fmt.Errorf("sql cache: write increment: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("sql cache: commit increment: %w", err)
	}
	return result, nil
}

// Expire replaces a live key's TTL. Zero clears expiration; expired and missing
// keys return ErrNotFound. A negative TTL returns ErrInvalidTTL.
func (c *Cache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if ttl < 0 {
		return cache.ErrInvalidTTL
	}
	if c.dialect != SQLite {
		return c.expireLocked(ctx, key, ttl)
	}
	query := `UPDATE hypercube_cache_entries
		SET expires_at_us = ` + c.dialect.nowMicros() + ` + ?
		WHERE storage_scope = ? AND cache_key = ?
		AND (expires_at_us IS NULL OR expires_at_us > ` + c.dialect.nowMicros() + `) RETURNING 1`
	var found int
	err := c.db.QueryRowContext(ctx, c.dialect.bind(query), ttlArgument(ttl),
		c.storageScope, hashKey(key)).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return cache.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("sql cache: expire: %w", err)
	}
	return nil
}

func (c *Cache) expireLocked(ctx context.Context, key string, ttl time.Duration) error {
	// Lock before checking expiration, and avoid relying on MySQL's affected
	// row count: an unchanged expiry is still a success (Expire(key, 0) twice).
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sql cache: begin expire: %w", err)
	}
	defer rollback(tx)
	keyHash := hashKey(key)
	var expiresAt sql.NullInt64
	query := `SELECT expires_at_us FROM hypercube_cache_entries
		WHERE storage_scope = ? AND cache_key = ? FOR UPDATE`
	err = tx.QueryRowContext(ctx, c.dialect.bind(query), c.storageScope, keyHash).Scan(&expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return cache.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("sql cache: lock expire: %w", err)
	}
	// Read time after acquiring the lock, so lock waits cannot revive a key
	// whose previous TTL elapsed while this operation was blocked.
	var now int64
	if err := tx.QueryRowContext(ctx, `SELECT `+c.dialect.nowMicros()).Scan(&now); err != nil {
		return fmt.Errorf("sql cache: read expiry clock: %w", err)
	}
	if expiresAt.Valid && expiresAt.Int64 <= now {
		return cache.ErrNotFound
	}
	query = `UPDATE hypercube_cache_entries
		SET expires_at_us = ` + c.dialect.nowMicros() + ` + ?
		WHERE storage_scope = ? AND cache_key = ?`
	if _, err := tx.ExecContext(ctx, c.dialect.bind(query), ttlArgument(ttl), c.storageScope, keyHash); err != nil {
		return fmt.Errorf("sql cache: expire: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sql cache: commit expire: %w", err)
	}
	return nil
}

// DeleteExpired removes expired rows in this cache's namespace. Reads already
// treat them as misses. Call this periodically using the application's scheduler.
func (c *Cache) DeleteExpired(ctx context.Context) (int64, error) {
	query := `DELETE FROM hypercube_cache_entries WHERE storage_scope = ?
		AND expires_at_us <= ` + c.dialect.nowMicros()
	result, err := c.db.ExecContext(ctx, c.dialect.bind(query), c.storageScope)
	if err != nil {
		return 0, fmt.Errorf("sql cache: delete expired: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sql cache: count expired: %w", err)
	}
	return count, nil
}

func hashKey(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func ttlArgument(ttl time.Duration) any {
	if ttl == 0 {
		return nil // Adding NULL to the server clock yields no expiration.
	}
	// Round up without overflowing for time.Duration's maximum value.
	micros := int64(ttl / time.Microsecond)
	if ttl%time.Microsecond > 0 {
		micros++
	}
	return micros
}

func rollback(tx *sql.Tx) { _ = tx.Rollback() }
