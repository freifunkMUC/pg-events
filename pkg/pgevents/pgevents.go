package pgevents

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // the database/sql driver used for the DDL
	"github.com/sirupsen/logrus"
)

// channel is the one every trigger notifies on. See "What the notifications
// expose" in the README.
const channel = "pgevents_event"

// pingInterval is how long the listener waits for a notification before it
// makes sure the connection is still there. A var so the tests can shorten it.
var pingInterval = time.Minute

// pingTimeout bounds that check: a connection that does not answer in this
// long is treated as gone, and the listener reconnects.
var pingTimeout = 10 * time.Second

// The delays between reconnection attempts, doubling from one to the next.
var (
	minReconnectDelay = time.Second
	maxReconnectDelay = time.Minute
)

type Listener struct {
	closeOnce sync.Once
	closeErr  error
	// cancel ends the goroutine that listens; done is closed once it has.
	cancel context.CancelFunc
	done   chan struct{}

	// db installs the function and the triggers; config opens the connection
	// that listens, which cannot come from a pool - it has to stay the same
	// connection for as long as it listens.
	db     *sql.DB
	config *pgx.ConnConfig

	// mu guards the callbacks: they may be registered while notifications
	// are already being delivered.
	mu                 sync.RWMutex
	eventCallbacks     []OnEvent
	reconnectCallbacks []OnReconnect
}

type TableEvent struct {
	Table  string `json:"table"`
	Action string `json:"action"`
	Data   string `json:"data"`
	// Truncated is set when the event carries no row: it did not fit into a
	// NOTIFY payload (8000 bytes), or the trigger was installed with
	// AttachWithoutRow. Data is empty then; read the row from the table if
	// you need it.
	Truncated bool `json:"truncated"`
}

// Action is a kind of table change a trigger reports.
type Action string

const (
	Insert Action = "INSERT"
	Update Action = "UPDATE"
	Delete Action = "DELETE"
)

type OnEvent func(*TableEvent)

type OnReconnect func()

// OpenListener connects, installs the notify function and starts listening.
// The context bounds that work - connecting to a database that is not there
// fails when it runs out - and nothing after it: the listener runs until
// Close, whatever becomes of the context.
func OpenListener(ctx context.Context, connectionString string) (*Listener, error) {
	config, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse the connection string: %w", err)
	}

	db, err := sql.Open("pgx", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open sql connection: %w", err)
	}

	if err := setup(ctx, db, procedure()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create postgres notify function: %w", err)
	}

	// The listener's own context, which Close cancels. It is deliberately not
	// derived from the one above: that one is about getting started.
	listenCtx, cancel := context.WithCancel(context.Background())
	l := &Listener{
		cancel: cancel,
		done:   make(chan struct{}),
		db:     db,
		config: config,
	}

	// The first connection is made here rather than in the background, so that
	// a connection that cannot be made is an error from OpenListener instead
	// of a line in the log every few seconds.
	conn, err := l.connect(ctx)
	if err != nil {
		cancel()
		_ = db.Close()
		return nil, err
	}

	go l.listen(listenCtx, conn)

	return l, nil
}

// Attach installs a trigger on table that reports inserts, updates and deletes.
func (l *Listener) Attach(ctx context.Context, table string) error {
	return l.AttachActions(ctx, table, Insert, Update, Delete)
}

// AttachActions installs a trigger on table that reports only the given
// actions. Every reported change is broadcast to every listening connection,
// so leaving out actions the application ignores saves that work.
//
// A table has a single pg-events trigger: attaching it again, with the same or
// other actions, replaces the previous one.
func (l *Listener) AttachActions(ctx context.Context, table string, actions ...Action) error {
	ordered, err := normalizeActions(actions)
	if err != nil {
		return fmt.Errorf("failed to attach listener: %w", err)
	}
	if err := setup(ctx, l.db, dropTrigger(table), createTrigger(table, ordered, true)); err != nil {
		return fmt.Errorf("failed to attach listener: %w", err)
	}
	return nil
}

// AttachWithoutRow installs a trigger that reports a change without the row in
// it: Data is empty and Truncated is set, as for a row too large to send. Read
// the row from the table when you need it.
//
// Use it for tables whose contents must not travel: a notification reaches
// every connection that listens on the channel, and table privileges do not
// apply to it. See "What the notifications expose" in the README.
func (l *Listener) AttachWithoutRow(ctx context.Context, table string, actions ...Action) error {
	ordered, err := normalizeActions(actions)
	if err != nil {
		return fmt.Errorf("failed to attach listener: %w", err)
	}
	if err := setup(ctx, l.db, dropTrigger(table), createTrigger(table, ordered, false)); err != nil {
		return fmt.Errorf("failed to attach listener: %w", err)
	}
	return nil
}

// normalizeActions validates actions and puts them in a stable order.
func normalizeActions(actions []Action) ([]Action, error) {
	if len(actions) == 0 {
		return nil, errors.New("no actions given")
	}
	wanted := map[Action]bool{}
	for _, action := range actions {
		switch action {
		case Insert, Update, Delete:
			wanted[action] = true
		default:
			return nil, fmt.Errorf("unknown action %q", action)
		}
	}
	var ordered []Action
	for _, action := range []Action{Insert, Update, Delete} {
		if wanted[action] {
			ordered = append(ordered, action)
		}
	}
	return ordered, nil
}

// setup runs DDL statements in one transaction while holding a
// transaction-scoped advisory lock, so applications starting at the same time
// do not install the function or triggers concurrently. The lock is released
// with the transaction, whatever happens.
func setup(ctx context.Context, db *sql.DB, statements ...string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", setupLockKey); err != nil {
		_ = tx.Rollback()
		return err
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (l *Listener) OnEvent(cb OnEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.eventCallbacks = append(l.eventCallbacks, cb)
}

func (l *Listener) OnReconnect(cb OnReconnect) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reconnectCallbacks = append(l.reconnectCallbacks, cb)
}

// connect opens a connection of its own and starts listening on the channel.
func (l *Listener) connect(ctx context.Context) (*pgx.Conn, error) {
	conn, err := pgx.ConnectConfig(ctx, l.config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("failed to listen to postgres events: %w", err)
	}
	return conn, nil
}

// listen delivers notifications until the listener is closed. A connection
// that breaks is replaced, and the reconnect callbacks are told: notifications
// sent while there was none are gone, so whoever keeps a copy of the data has
// to read it again.
func (l *Listener) listen(ctx context.Context, conn *pgx.Conn) {
	defer close(l.done)
	defer func() {
		if conn != nil {
			l.disconnect(conn)
		}
	}()

	for {
		if conn == nil {
			var err error
			conn, err = l.reconnect(ctx)
			if err != nil {
				return // the listener was closed while it was reconnecting
			}
			logrus.Info("pgevents reconnected to postgres")
			l.emitReconnect()
		}

		if l.receive(ctx, conn) {
			continue
		}

		l.disconnect(conn)
		conn = nil
		if ctx.Err() != nil {
			return
		}
	}
}

// receive waits for one notification and reports whether the connection can
// keep being used.
func (l *Listener) receive(ctx context.Context, conn *pgx.Conn) bool {
	// The wait is bounded so that a quiet connection is checked now and then:
	// one that went away without a FIN would otherwise be waited on forever.
	// A wait that runs out leaves the connection usable - pgx only discards it
	// on errors that are not timeouts.
	waitCtx, cancel := context.WithTimeout(ctx, pingInterval)
	notification, err := conn.WaitForNotification(waitCtx)
	cancel()

	switch {
	case err == nil:
		logrus.Debugf("received data from channel: %s", notification.Channel)
		l.emitEvent(notification)
		return true

	case ctx.Err() != nil:
		return false

	case pgconn.Timeout(err):
		logrus.Debug("no events received for a while: checking the connection")
		pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
		defer cancel()
		if err := conn.Ping(pingCtx); err != nil {
			logrus.Warn(fmt.Errorf("pgevents lost its postgres connection: %w", err))
			return false
		}
		return true

	default:
		logrus.Warn(fmt.Errorf("pgevents lost its postgres connection: %w", err))
		return false
	}
}

// reconnect opens a new connection, waiting longer between attempts as they
// keep failing. It returns an error only when the listener was closed.
func (l *Listener) reconnect(ctx context.Context) (*pgx.Conn, error) {
	delay := minReconnectDelay
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}

		conn, err := l.connect(ctx)
		if err == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		logrus.Warn(fmt.Errorf("pgevents failed to reconnect to postgres: %w", err))
		if delay *= 2; delay > maxReconnectDelay {
			delay = maxReconnectDelay
		}
	}
}

// disconnect closes a connection that will not be used again. The listener's
// own context may be cancelled by then, which is why closing does not use it.
func (l *Listener) disconnect(conn *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	if err := conn.Close(ctx); err != nil {
		logrus.Debugf("closing the postgres connection: %v", err)
	}
}

func (l *Listener) emitEvent(notification *pgconn.Notification) {
	event := &TableEvent{}

	if err := json.Unmarshal([]byte(notification.Payload), event); err != nil {
		logrus.Error(fmt.Errorf("failed to unmarshal table event: %w", err))
		return
	}

	// copy under the lock and call outside it, so a callback may register
	// further callbacks without deadlocking
	l.mu.RLock()
	callbacks := append([]OnEvent(nil), l.eventCallbacks...)
	l.mu.RUnlock()

	for _, cb := range callbacks {
		safely(func() { cb(event) })
	}
}

func (l *Listener) emitReconnect() {
	l.mu.RLock()
	callbacks := append([]OnReconnect(nil), l.reconnectCallbacks...)
	l.mu.RUnlock()

	for _, cb := range callbacks {
		safely(cb)
	}
}

// safely runs a callback of the application. A panic in one used to end the
// goroutine that delivers events, which left the listener connected and
// silent: no further event reached any callback, and nothing said why.
func safely(cb func()) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("pgevents: a callback panicked: %v\n%s", r, debug.Stack())
		}
	}()
	cb()
}

// Close stops delivering events and closes the connections. It waits until a
// callback that is currently running has returned, so it must not be called
// from inside a callback. Calling it more than once is safe.
func (l *Listener) Close() error {
	l.closeOnce.Do(func() {
		l.cancel()
		<-l.done
		l.closeErr = l.db.Close()
	})
	return l.closeErr
}
