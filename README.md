# pg-events

This is a small library for using Postgres LISTEN/NOTIFY to subscribe
to table change events.

When a row is inserted, updated or deleted postgres will notify
your application and your custom callbacks will be invoked allowing
you to respond to the event anyway you like.

The row is provided to your callback as a JSON string.

Here's a quick example:

```golang
func main() {
	connectionString := "host=localhost port=5432 ..."

	// connect to postgres
	listener, err := pgevents.OpenListener(connectionString)
	if err != nil {
		log.Fatal(err)
	}

	// attach the listener to 1 or more table(s)
	if err := listener.Attach("my_table"); err != nil {
		log.Fatal(err)
	}

	// attach 1 or more callback(s)
	listener.OnEvent(func(event *pgevents.TableEvent) {
		fmt.Printf("received event: %v\n", event)
	})
}
```

## Choosing which changes are reported

`Attach` reports inserts, updates and deletes. Every reported change is broadcast
to every listening connection, so if your application ignores some of them, attach
only the ones you need:

```golang
listener.AttachActions("my_table", pgevents.Insert, pgevents.Delete)
```

A table has one pg-events trigger; attaching it again replaces the previous one.

## Large rows

Postgres rejects NOTIFY payloads of 8000 bytes or more. Instead of letting that
error abort your write, pg-events then sends the event without the row: `Data` is
empty and `Truncated` is `true`. Read the row from the table if you need it.

## Table names

A name that is a valid unquoted SQL identifier is folded to lower case, exactly as
Postgres does, so `Attach("MyTable")` refers to `mytable`. Names that need quoting,
such as ones with spaces or reserved words like `user`, work as given. To refer to a
case-sensitive table, pass the name in double quotes: `Attach("\"MyTable\"")`.

## Running several applications against one database

`OpenListener` and `Attach` install their function and trigger under an advisory
lock, so several instances starting at the same time do not fail on each other's
changes. Applications using an older pg-events version do not take that lock; during
a rolling upgrade from one, a start can still fail once and succeed on retry.

Calling `OnEvent`, `OnReconnect` and `Close` concurrently with event delivery is
safe, and `Close` may be called more than once. Do not call `Close` from inside a
callback: it waits for running callbacks to return.

## Tests

The tests need a real Postgres. `run-database.sh` starts one in Docker and prints
the command to run them.
