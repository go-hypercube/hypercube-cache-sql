package sqlcache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-hypercube/go-hypercube/cache"
	_ "github.com/mattn/go-sqlite3"
)

func openDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { must(t, db.Close()) })
	return db
}

func newCache(t *testing.T, db *sql.DB, options ...Option) *Cache {
	t.Helper()
	c, err := New(db, SQLite, options...)
	must(t, err)
	must(t, c.Migrate(t.Context()))
	return c
}

func testCache(t *testing.T) (*Cache, *sql.DB) {
	t.Helper()
	db := openDatabase(t, filepath.Join(t.TempDir(), "cache.db"))
	return newCache(t, db), db
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func wantError(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want %v", err, target)
	}
}

func wantValue(t *testing.T, c *Cache, key, want string) {
	t.Helper()
	got, err := c.Get(t.Context(), key)
	must(t, err)
	if got != want {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
}

func expiry(t *testing.T, c *Cache, key string) sql.NullInt64 {
	t.Helper()
	var value sql.NullInt64
	must(t, c.db.QueryRowContext(t.Context(), `SELECT expires_at_us FROM hypercube_cache_entries
		WHERE storage_scope = ? AND cache_key = ?`, c.storageScope, hashKey(key)).Scan(&value))
	return value
}

func forceExpired(t *testing.T, c *Cache, key string) {
	t.Helper()
	_, err := c.db.ExecContext(t.Context(), `UPDATE hypercube_cache_entries SET expires_at_us = 0
		WHERE storage_scope = ? AND cache_key = ?`, c.storageScope, hashKey(key))
	must(t, err)
}

func TestNewAndMigrate(t *testing.T) {
	_, err := New(nil, SQLite)
	wantError(t, err, ErrNilDatabase)
	c, db := testCache(t)
	must(t, c.Migrate(t.Context()))
	_, err = New(db, Dialect("oracle"))
	wantError(t, err, ErrUnsupportedDialect)
	for _, dialect := range []Dialect{PostgreSQL, MySQL, SQLite} {
		// Construction validates configuration without issuing SQL.
		_, err := New(db, dialect, nil)
		must(t, err)
	}
	must(t, db.Close())
	_, err = New(db, SQLite)
	must(t, err)
	if err := c.Migrate(t.Context()); err == nil {
		t.Fatal("migration on a closed database succeeded")
	}
}

func TestCacheContract(t *testing.T) {
	c, _ := testCache(t)
	ctx := t.Context()
	_, err := c.Get(ctx, "missing")
	wantError(t, err, cache.ErrNotFound)
	has, err := c.Has(ctx, "missing")
	must(t, err)
	if has {
		t.Fatal("missing key exists")
	}
	must(t, c.Delete(ctx, "missing"))
	wantError(t, c.Expire(ctx, "missing", time.Hour), cache.ErrNotFound)

	for _, key := range []string{"", "key", "KEY", "key ", "a' OR 1=1 --", "a\x00\xff", "مفتاح", strings.Repeat("long", 4096)} {
		value := "value:" + key + "\x00\xff"
		must(t, c.Set(ctx, key, value, 0))
		wantValue(t, c, key, value)
	}
	wantValue(t, c, "key", "value:key\x00\xff")
	must(t, c.Set(ctx, "key", "", 0))
	wantValue(t, c, "key", "")
	has, err = c.Has(ctx, "key")
	must(t, err)
	if !has {
		t.Fatal("empty value should exist")
	}
	must(t, c.Delete(ctx, "key"))
	_, err = c.Get(ctx, "key")
	wantError(t, err, cache.ErrNotFound)
	wantValue(t, c, "KEY", "value:KEY\x00\xff")
}

func TestTTLSemantics(t *testing.T) {
	c, _ := testCache(t)
	ctx := t.Context()
	must(t, c.Set(ctx, "key", "old", time.Hour))
	if !expiry(t, c, "key").Valid {
		t.Fatal("positive TTL did not set expiration")
	}
	wantError(t, c.Set(ctx, "key", "new", -1), cache.ErrInvalidTTL)
	wantError(t, c.Expire(ctx, "key", -1), cache.ErrInvalidTTL)
	wantValue(t, c, "key", "old")

	must(t, c.Set(ctx, "key", "new", 0))
	if expiry(t, c, "key").Valid {
		t.Fatal("Set with zero TTL did not clear expiration")
	}
	must(t, c.Expire(ctx, "key", time.Hour))
	first := expiry(t, c, "key")
	must(t, c.Expire(ctx, "key", 2*time.Hour))
	if second := expiry(t, c, "key"); !second.Valid || second.Int64 <= first.Int64 {
		t.Fatal("Expire did not replace TTL")
	}
	must(t, c.Expire(ctx, "key", 0))
	must(t, c.Expire(ctx, "key", 0))
	if expiry(t, c, "key").Valid {
		t.Fatal("Expire with zero TTL did not clear expiration")
	}

	// Move the stored deadline into the past rather than sleeping.
	forceExpired(t, c, "key")
	_, err := c.Get(ctx, "key")
	wantError(t, err, cache.ErrNotFound)
	has, err := c.Has(ctx, "key")
	must(t, err)
	if has {
		t.Fatal("expired key exists")
	}
	wantError(t, c.Expire(ctx, "key", time.Hour), cache.ErrNotFound)
	wantError(t, c.Expire(ctx, "key", 0), cache.ErrNotFound)
	must(t, c.Set(ctx, "key", "replaced", time.Hour))
	wantValue(t, c, "key", "replaced")

	// The clock comes from SQL, and even maximum durations stay representable.
	var before int64
	must(t, c.db.QueryRowContext(ctx, `SELECT `+c.dialect.nowMicros()).Scan(&before))
	must(t, c.Set(ctx, "maximum", "value", time.Duration(math.MaxInt64)))
	if got := expiry(t, c, "maximum"); !got.Valid || got.Int64 < before+int64(math.MaxInt64/time.Microsecond) {
		t.Fatalf("maximum TTL deadline = %v", got)
	}
}

func TestTTLPrecision(t *testing.T) {
	for _, tc := range []struct {
		ttl  time.Duration
		want any
	}{
		{0, nil}, {time.Nanosecond, int64(1)}, {time.Microsecond, int64(1)},
		{time.Microsecond + time.Nanosecond, int64(2)},
		{time.Duration(math.MaxInt64), int64(math.MaxInt64/time.Microsecond) + 1},
	} {
		if got := ttlArgument(tc.ttl); got != tc.want {
			t.Errorf("ttlArgument(%v) = %v, want %v", tc.ttl, got, tc.want)
		}
	}
}

func TestIncrement(t *testing.T) {
	c, _ := testCache(t)
	ctx := t.Context()
	for _, tc := range []struct {
		key   string
		delta int64
		want  int64
	}{
		{"new", 3, 3}, {"new", -5, -2}, {"zero", 0, 0},
		{"max", math.MaxInt64, math.MaxInt64}, {"min", math.MinInt64, math.MinInt64},
	} {
		got, err := c.Increment(ctx, tc.key, tc.delta)
		must(t, err)
		if got != tc.want {
			t.Fatalf("Increment(%q, %d) = %d, want %d", tc.key, tc.delta, got, tc.want)
		}
		wantValue(t, c, tc.key, strconv.FormatInt(tc.want, 10))
	}
	must(t, c.Set(ctx, "live", "00010", time.Hour))
	deadline := expiry(t, c, "live")
	got, err := c.Increment(ctx, "live", 2)
	must(t, err)
	if got != 12 || expiry(t, c, "live") != deadline {
		t.Fatal("increment must preserve a live TTL")
	}
	must(t, c.Set(ctx, "expired", "not an integer", time.Hour))
	forceExpired(t, c, "expired")
	got, err = c.Increment(ctx, "expired", -2)
	must(t, err)
	if got != -2 || expiry(t, c, "expired").Valid {
		t.Fatal("increment must reset an expired key from zero without TTL")
	}

	for _, tc := range []struct {
		value string
		delta int64
	}{
		{"", 1}, {"1.5", 1}, {" 1", 1}, {"1x", 1}, {"0xff", 1},
		{"9223372036854775808", 0}, {"9223372036854775807", 1},
		{"-9223372036854775808", -1},
	} {
		must(t, c.Set(ctx, "invalid", tc.value, time.Hour))
		deadline := expiry(t, c, "invalid")
		_, err := c.Increment(ctx, "invalid", tc.delta)
		wantError(t, err, ErrInvalidInteger)
		wantValue(t, c, "invalid", tc.value)
		if expiry(t, c, "invalid") != deadline {
			t.Fatal("failed increment changed TTL")
		}
	}
}

func TestPersistenceAndNamespaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	db := openDatabase(t, path)
	c := newCache(t, db)
	ctx := t.Context()
	must(t, c.Set(ctx, "persisted", "value", 0))
	must(t, c.Set(ctx, "ttl", "value", time.Hour))
	deadline := expiry(t, c, "ttl")
	must(t, db.Close())

	reopened := newCache(t, openDatabase(t, path))
	wantValue(t, reopened, "persisted", "value")
	wantValue(t, reopened, "ttl", "value")
	if expiry(t, reopened, "ttl") != deadline {
		t.Fatal("TTL changed on reopening")
	}
	shared := newCache(t, openDatabase(t, path))
	must(t, shared.Set(ctx, "persisted", "updated", 0))
	wantValue(t, reopened, "persisted", "updated")

	other := newCache(t, reopened.db, WithStorageNamespace("other\x00\xff"))
	_, err := other.Get(ctx, "persisted")
	wantError(t, err, cache.ErrNotFound)
	must(t, other.Set(ctx, "persisted", "isolated", 0))
	must(t, other.Delete(ctx, "persisted"))
	wantValue(t, reopened, "persisted", "updated")
}

func TestDeleteExpired(t *testing.T) {
	c, db := testCache(t)
	other := newCache(t, db, WithStorageNamespace("other"))
	ctx := t.Context()
	for _, key := range []string{"expired", "live", "immortal"} {
		must(t, c.Set(ctx, key, key, time.Hour))
	}
	must(t, c.Expire(ctx, "immortal", 0))
	forceExpired(t, c, "expired")
	must(t, other.Set(ctx, "expired", "other", 0))
	forceExpired(t, other, "expired")
	count, err := c.DeleteExpired(ctx)
	must(t, err)
	if count != 1 {
		t.Fatalf("deleted %d rows, want 1", count)
	}
	wantValue(t, c, "live", "live")
	wantValue(t, c, "immortal", "immortal")
	count, err = c.DeleteExpired(ctx)
	must(t, err)
	if count != 0 {
		t.Fatalf("second cleanup deleted %d rows", count)
	}
	count, err = other.DeleteExpired(ctx)
	must(t, err)
	if count != 1 {
		t.Fatal("cleanup removed another namespace's expired row")
	}
}

func TestConcurrentIncrementsAcrossPools(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	caches := []*Cache{newCache(t, openDatabase(t, path)), newCache(t, openDatabase(t, path))}
	const workers, iterations = 8, 25
	start := make(chan struct{})
	results := make(chan int64, workers*iterations)
	errors := make(chan error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			<-start
			for range iterations {
				n, err := caches[i%len(caches)].Increment(t.Context(), "counter", 1)
				if err != nil {
					errors <- err
					return
				}
				results <- n
			}
		})
	}
	close(start)
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	seen := make(map[int64]bool)
	for n := range results {
		if n < 1 || n > workers*iterations || seen[n] {
			t.Errorf("non-atomic increment result %d", n)
		}
		seen[n] = true
	}
	if len(seen) != workers*iterations {
		t.Fatalf("got %d distinct increment results", len(seen))
	}
	wantValue(t, caches[0], "counter", strconv.Itoa(workers*iterations))
}

func TestContextAndDatabaseErrors(t *testing.T) {
	c, db := testCache(t)
	operations := map[string]func(context.Context) error{
		"migrate":   c.Migrate,
		"get":       func(ctx context.Context) error { _, err := c.Get(ctx, "key"); return err },
		"set":       func(ctx context.Context) error { return c.Set(ctx, "key", "1", 0) },
		"delete":    func(ctx context.Context) error { return c.Delete(ctx, "key") },
		"has":       func(ctx context.Context) error { _, err := c.Has(ctx, "key"); return err },
		"increment": func(ctx context.Context) error { _, err := c.Increment(ctx, "key", 1); return err },
		"expire":    func(ctx context.Context) error { return c.Expire(ctx, "key", 0) },
		"cleanup":   func(ctx context.Context) error { _, err := c.DeleteExpired(ctx); return err },
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for name, operation := range operations {
		t.Run("cancelled/"+name, func(t *testing.T) {
			wantError(t, operation(ctx), context.Canceled)
		})
	}
	must(t, db.Close())
	for name, operation := range operations {
		t.Run("closed/"+name, func(t *testing.T) {
			err := operation(t.Context())
			if err == nil || errors.Is(err, cache.ErrNotFound) {
				t.Fatalf("closed database error = %v", err)
			}
		})
	}
}

func ExampleNew() {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	c, err := New(db, SQLite)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	if err := c.Migrate(ctx); err != nil {
		panic(err)
	}
	if err := c.Set(ctx, "greeting", "hello", 0); err != nil {
		panic(err)
	}
	value, err := c.Get(ctx, "greeting")
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
	// Output: hello
}
