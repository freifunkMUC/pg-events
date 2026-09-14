package pgevents

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// These tests need a real Postgres server, e.g. started with run-database.sh:
// PGEVENTS_TEST_DATABASE_URL="postgres://postgres:development@localhost:5432/postgres?sslmode=disable"
func testDatabase(t *testing.T) string {
	t.Helper()
	url := os.Getenv("PGEVENTS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("PGEVENTS_TEST_DATABASE_URL not set")
	}
	return url
}

func openListener(t *testing.T, url string) *Listener {
	t.Helper()
	l, err := OpenListener(url)
	if err != nil {
		t.Fatalf("OpenListener: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func exec(t *testing.T, url string, statements ...string) {
	t.Helper()
	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

// uniqueTable returns a table name no other test uses and drops it afterwards.
func uniqueTable(t *testing.T, url string) string {
	t.Helper()
	name := fmt.Sprintf("pgevents_test_%d", time.Now().UnixNano())
	exec(t, url, fmt.Sprintf("CREATE TABLE %s (id serial PRIMARY KEY, name text)", name))
	t.Cleanup(func() { exec(t, url, "DROP TABLE IF EXISTS "+name) })
	return name
}

func collect(l *Listener) (func() []*TableEvent, *sync.Mutex) {
	var mu sync.Mutex
	var events []*TableEvent
	l.OnEvent(func(e *TableEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})
	return func() []*TableEvent {
		mu.Lock()
		defer mu.Unlock()
		return append([]*TableEvent(nil), events...)
	}, &mu
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Several applications starting at the same time all install the notify
// function. CREATE OR REPLACE FUNCTION run concurrently fails in Postgres with
// "tuple concurrently updated", which used to abort the application's start.
func TestConcurrentOpenListener(t *testing.T) {
	url := testDatabase(t)
	table := uniqueTable(t, url)

	const replicas = 8
	for round := 0; round < 5; round++ {
		var wg sync.WaitGroup
		errs := make(chan error, replicas)
		for i := 0; i < replicas; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				l, err := OpenListener(url)
				if err != nil {
					errs <- err
					return
				}
				defer l.Close()
				if err := l.Attach(table); err != nil {
					errs <- err
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Errorf("round %d: %v", round, err)
		}
	}
}

func TestAttachNotifiesInsertUpdateDelete(t *testing.T) {
	url := testDatabase(t)
	table := uniqueTable(t, url)
	l := openListener(t, url)
	if err := l.Attach(table); err != nil {
		t.Fatal(err)
	}
	events, _ := collect(l)

	exec(t, url,
		fmt.Sprintf("INSERT INTO %s (name) VALUES ('a')", table),
		fmt.Sprintf("UPDATE %s SET name = 'b'", table),
		fmt.Sprintf("DELETE FROM %s", table),
	)
	waitFor(t, "three events", func() bool { return len(events()) == 3 })

	var actions []string
	for _, e := range events() {
		actions = append(actions, e.Action)
	}
	if got := strings.Join(actions, ","); got != "INSERT,UPDATE,DELETE" {
		t.Errorf("actions = %s", got)
	}
}

// Rows larger than NOTIFY's payload limit used to make the write itself fail,
// because the trigger error aborts the INSERT or UPDATE.
func TestOversizedRowDoesNotBreakWrites(t *testing.T) {
	url := testDatabase(t)
	table := uniqueTable(t, url)
	l := openListener(t, url)
	if err := l.Attach(table); err != nil {
		t.Fatal(err)
	}
	events, _ := collect(l)

	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (name) VALUES ($1)", table), strings.Repeat("x", 10000)); err != nil {
		t.Fatalf("a large row must still be writable: %v", err)
	}

	waitFor(t, "an event for the large row", func() bool { return len(events()) == 1 })
	e := events()[0]
	if e.Action != "INSERT" || e.Table != table {
		t.Errorf("event = %+v", e)
	}
	if !e.Truncated || e.Data != "" {
		t.Errorf("a row too large for NOTIFY must arrive truncated without data, got Truncated=%v len(Data)=%d", e.Truncated, len(e.Data))
	}
}

func TestAttachQuotesTableNames(t *testing.T) {
	url := testDatabase(t)
	name := fmt.Sprintf("PgEvents Mixed %d", time.Now().UnixNano())
	exec(t, url, fmt.Sprintf(`CREATE TABLE "%s" (id serial PRIMARY KEY)`, name))
	t.Cleanup(func() { exec(t, url, fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, name)) })

	l := openListener(t, url)
	if err := l.Attach(name); err != nil {
		t.Fatalf("Attach(%q): %v", name, err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	url := testDatabase(t)
	l, err := OpenListener(url)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		_ = l.Close()
		_ = l.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a second Close blocked")
	}
}

// Registering callbacks while notifications are already being delivered must
// be safe: OpenListener starts delivering before the caller can add callbacks,
// and the README registers them afterwards.
func TestOnEventWhileEventsArrive(t *testing.T) {
	url := testDatabase(t)
	table := uniqueTable(t, url)
	l := openListener(t, url)
	if err := l.Attach(table); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = db.Exec(fmt.Sprintf("INSERT INTO %s (name) VALUES ('x')", table))
			}
		}
	}()

	for i := 0; i < 200; i++ {
		l.OnEvent(func(*TableEvent) {})
		l.OnReconnect(func() {})
		time.Sleep(time.Millisecond)
	}
	close(stop)
	<-writerDone
}

func TestAttachActionsReportsOnlyThoseActions(t *testing.T) {
	url := testDatabase(t)
	table := uniqueTable(t, url)
	l := openListener(t, url)
	// attaching again replaces the trigger instead of adding a second one
	if err := l.Attach(table); err != nil {
		t.Fatal(err)
	}
	if err := l.AttachActions(table, Delete, Insert); err != nil {
		t.Fatal(err)
	}
	events, _ := collect(l)

	exec(t, url,
		fmt.Sprintf("INSERT INTO %s (name) VALUES ('a')", table),
		fmt.Sprintf("UPDATE %s SET name = 'b'", table),
		fmt.Sprintf("DELETE FROM %s", table),
	)
	waitFor(t, "insert and delete", func() bool { return len(events()) >= 2 })
	time.Sleep(300 * time.Millisecond) // give a stray UPDATE event time to show up

	var actions []string
	for _, e := range events() {
		actions = append(actions, e.Action)
	}
	if got := strings.Join(actions, ","); got != "INSERT,DELETE" {
		t.Errorf("actions = %s, want INSERT,DELETE", got)
	}
}

func TestAttachActionsRejectsInvalidInput(t *testing.T) {
	url := testDatabase(t)
	table := uniqueTable(t, url)
	l := openListener(t, url)
	if err := l.AttachActions(table); err == nil {
		t.Error("no actions must be an error")
	}
	if err := l.AttachActions(table, "TRUNCATE"); err == nil {
		t.Error("an unknown action must be an error")
	}
}

// Earlier versions interpolated the table name unquoted, so Postgres folded it
// to lower case. Callers relying on that must keep working.
func TestAttachKeepsUnquotedNameFolding(t *testing.T) {
	url := testDatabase(t)
	name := fmt.Sprintf("PgEventsFolded%d", time.Now().UnixNano())
	exec(t, url, fmt.Sprintf("CREATE TABLE %s (id serial PRIMARY KEY)", name)) // folded to lower case
	t.Cleanup(func() { exec(t, url, "DROP TABLE IF EXISTS "+name) })

	l := openListener(t, url)
	if err := l.Attach(name); err != nil {
		t.Fatalf("Attach(%q) of an unquoted mixed-case table: %v", name, err)
	}
	events, _ := collect(l)
	exec(t, url, fmt.Sprintf("INSERT INTO %s DEFAULT VALUES", name))
	waitFor(t, "an event", func() bool { return len(events()) == 1 })
	if want := strings.ToLower(name); events()[0].Table != want {
		t.Errorf("table = %q, want %q", events()[0].Table, want)
	}
}

func TestIdentifier(t *testing.T) {
	for in, want := range map[string]string{
		"devices":            `"devices"`,
		"MyTable":            `"mytable"`,
		"user":               `"user"`,
		"with space":         `"with space"`,
		`"CaseKept"`:         `"CaseKept"`,
		`"has ""quote"" in"`: `"has ""quote"" in"`,
		`odd"name`:           `"odd""name"`,
	} {
		if got := identifier(in); got != want {
			t.Errorf("identifier(%q) = %s, want %s", in, got, want)
		}
	}
	if got := triggerName("MyTable"); got != `"mytable_events"` {
		t.Errorf("triggerName = %s", got)
	}
}
