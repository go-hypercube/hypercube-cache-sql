# hypercube-cache-sql

A persistent [`database/sql`](https://pkg.go.dev/database/sql) provider for
[go-hypercube's `cache.Cache`](https://pkg.go.dev/github.com/go-hypercube/go-hypercube/cache).
Reuse your application's SQL database when Redis or another cache service is
unavailable. Entries survive application restarts and are shared by instances
using the same database and storage namespace.

Implements `Get`, `Set`, `Delete`, `Has`, `Increment`, and `Expire` without changes
to the framework's cache interface.

## Supported databases

| Dialect | Requirement |
| --- | --- |
| `sqlcache.PostgreSQL` | PostgreSQL 9.5+ |
| `sqlcache.MySQL` | MySQL 8.0+ with InnoDB |
| `sqlcache.SQLite` | SQLite 3.35+ (`UPDATE RETURNING`) |

The application registers its own `database/sql` driver. This package has no
production dependency on a SQL driver; SQLite is used by its tests only.

## Usage

```go
import (
    "context"
    "database/sql"
    "time"

    sqlcache "github.com/go-hypercube/hypercube-cache-sql"
)

// db is your application's existing *sql.DB, with its driver registered.
func createCache(ctx context.Context, db *sql.DB) (*sqlcache.Cache, error) {
    store, err := sqlcache.New(db, sqlcache.PostgreSQL,
        sqlcache.WithStorageNamespace("myapp"),
    )
    if err != nil {
        return nil, err
    }
    if err := store.Migrate(ctx); err != nil {
        return nil, err
    }
    return store, nil
}

// Cache values use the same API as the memory and Redis providers:
func cacheGreeting(ctx context.Context, store *sqlcache.Cache) error {
    return store.Set(ctx, "greeting", "hello", 5*time.Minute)
}
```

Pass the resulting store into the framework's existing application options:

```go
application, err := app.New(&app.Options{
    Config:   cfg,
    Database: db,
    Cache:    store,
    Queue:    q,
    Logger:   logger,
})
```

`New` validates local configuration only. Call `Migrate(ctx)` during startup or
deployment before serving cache traffic. It creates the
`hypercube_cache_entries` table and expiry index using idempotent DDL. Projects
with an external migration system can use the statements in `dialect.go` instead.

The caller owns `*sql.DB` and closes it after cache operations stop. The provider
does not start goroutines or close the database. All database operations use the
supplied context; configure application deadlines and driver connection timeouts.

## TTLs and counters

- Zero TTL means no expiration. Negative TTLs return `cache.ErrInvalidTTL`.
- `Set` replaces both the value and TTL. `Expire` changes only a live key's TTL;
  zero clears its expiration. Missing or expired keys return `cache.ErrNotFound`.
- `Get` returns `cache.ErrNotFound` for expired or missing keys. `Has` returns
  `(false, nil)` for either case. `Delete` succeeds for a missing key.
- `Increment` creates missing or expired keys from zero without an expiration.
  For a live key, it preserves the exact stored expiry. Increments use database
  write locks and transactions so separate application instances cannot lose
  updates. Results are returned after commit.
- Integers are parsed in Go as signed base-10 `int64` values. Invalid values and
  overflow return `sqlcache.ErrInvalidInteger` and leave the entry unchanged.
  Errors can be checked with `errors.Is`.

Expiration uses database-server time, with deadlines stored as epoch
microseconds. Positive TTLs round up to a microsecond. SQLite's effective clock
resolution is usually milliseconds. Deadlines persist across restarts and avoid
clock skew between application hosts.

Keys and storage namespaces use SHA-256 fingerprints so long or binary strings
do not depend on database text collations or index length limits. Values use
binary columns and preserve empty strings, NUL, and invalid UTF-8 bytes.

## Expired-row cleanup

Expired entries are immediately invisible to reads but remain stored until
overwritten, deleted, or cleaned up. Call `DeleteExpired` periodically from your
application's scheduler to reclaim storage:

```go
deleted, err := store.DeleteExpired(ctx)
```

Cleanup affects only the configured storage namespace. The default namespace is
`hypercube:cache`; use `WithStorageNamespace` to isolate applications sharing a
database. Cleanup uses one DELETE statement, so choose a cadence that keeps each
cleanup small enough for your database workload.

## Deployment and testing

A SQL cache adds database traffic and shares the database's availability and
durability settings. SQLite writers serialize; for concurrent access configure a
finite busy timeout and usually WAL mode (for example, mattn/go-sqlite3 DSN
options `_busy_timeout=5000&_journal_mode=WAL`). A plain SQLite `:memory:` DSN
requires `db.SetMaxOpenConns(1)` to keep all operations on the same database.

Database errors are propagated without automatic retries. If a connection fails
or a context is cancelled during a write, its outcome may be unknown; retrying
`Increment` can add the delta again.

The test suite uses temporary SQLite files and covers persistence, TTLs,
namespace isolation, and concurrent counters through independent connection
pools. It requires CGO and a C compiler for the test-only SQLite driver:

```sh
make test
go test -race ./...
go vet ./...
```

Run integration checks against your PostgreSQL or MySQL server and driver before
deploying with those dialects.

## License

MIT
