package pgevents

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

// pingInterval is how often the connection is checked. A var so the tests can
// shorten it.
var pingInterval = time.Minute

type Listener struct {
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
	db        *sql.DB
	pql       *pq.Listener

	// mu guards the callbacks: they may be registered while notifications
	// are already being delivered.
	mu                 sync.RWMutex
	eventCallbacks     []OnEvent
	reconnectCallbacks []OnReconnect

	// pinging is set while a connection check is running, so they cannot pile
	// up behind one that blocks.
	pinging atomic.Bool
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

func OpenListener(connectionString string) (*Listener, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open sql connection: %w", err)
	}

	if err := setup(db, procedure()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create postgres notify function: %w", err)
	}

	l := &Listener{
		stop: make(chan struct{}),
		done: make(chan struct{}),
		db:   db,
		pql:  pq.NewListener(connectionString, 10*time.Second, time.Minute, logListenerEvent),
	}

	if err := l.start(); err != nil {
		_ = l.pql.Close()
		_ = db.Close()
		return nil, err
	}

	return l, nil
}

// Attach installs a trigger on table that reports inserts, updates and deletes.
func (l *Listener) Attach(table string) error {
	return l.AttachActions(table, Insert, Update, Delete)
}

// AttachActions installs a trigger on table that reports only the given
// actions. Every reported change is broadcast to every listening connection,
// so leaving out actions the application ignores saves that work.
//
// A table has a single pg-events trigger: attaching it again, with the same or
// other actions, replaces the previous one.
func (l *Listener) AttachActions(table string, actions ...Action) error {
	ordered, err := normalizeActions(actions)
	if err != nil {
		return fmt.Errorf("failed to attach listener: %w", err)
	}
	if err := setup(l.db, dropTrigger(table), createTrigger(table, ordered, true)); err != nil {
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
func (l *Listener) AttachWithoutRow(table string, actions ...Action) error {
	ordered, err := normalizeActions(actions)
	if err != nil {
		return fmt.Errorf("failed to attach listener: %w", err)
	}
	if err := setup(l.db, dropTrigger(table), createTrigger(table, ordered, false)); err != nil {
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
func setup(db *sql.DB, statements ...string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec("SELECT pg_advisory_xact_lock($1)", setupLockKey); err != nil {
		_ = tx.Rollback()
		return err
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
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

func logListenerEvent(event pq.ListenerEventType, err error) {
	switch event {
	case pq.ListenerEventDisconnected:
		logrus.Warn(fmt.Errorf("pgevents lost its postgres connection: %w", err))
	case pq.ListenerEventConnectionAttemptFailed:
		logrus.Warn(fmt.Errorf("pgevents failed to reconnect to postgres: %w", err))
	case pq.ListenerEventReconnected:
		logrus.Info("pgevents reconnected to postgres")
	}
}

func (l *Listener) start() error {
	if err := l.pql.Listen("pgevents_event"); err != nil {
		return fmt.Errorf("failed to listen to postgres events: %w", err)
	}

	go func() {
		defer close(l.done)
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-l.stop:
				logrus.Debug("finished listening for events")
				return
			case notification := <-l.pql.NotificationChannel():
				if notification != nil {
					logrus.Debugf("received data from channel: %s", notification.Channel)
					l.emitEvent(notification)
				} else {
					// a nil notification is documented to mean that
					// the connection has been lost and then re-established
					// i.e. a reconnect occurred and some notifications may
					// have been missed.
					logrus.Debug("received nil from channel indicating reconnect")
					l.emitReconnect()
				}
			case <-ticker.C:
				// One check at a time: a dead connection makes Ping block, and
				// a goroutine per tick would pile up behind it.
				if l.pinging.CompareAndSwap(false, true) {
					go func() {
						defer l.pinging.Store(false)
						logrus.Debug("checking the postgres connection")
						if err := l.pql.Ping(); err != nil {
							logrus.Error(fmt.Errorf("pgevents ping returned an error: %w", err))
						}
					}()
				}
			}
		}
	}()

	return nil
}

func (l *Listener) emitEvent(notification *pq.Notification) {
	event := &TableEvent{}

	if err := json.Unmarshal([]byte(notification.Extra), event); err != nil {
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
		close(l.stop)
		<-l.done
		if err := l.pql.Close(); err != nil {
			l.closeErr = err
		}
		if err := l.db.Close(); err != nil && l.closeErr == nil {
			l.closeErr = err
		}
	})
	return l.closeErr
}
