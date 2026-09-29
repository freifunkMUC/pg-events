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

## What the notifications expose

A notification reaches **every connection that listens on the channel**, and Postgres does not apply
table privileges to it. A user who may connect to the database but may not read the table still sees
every row it writes:

```
postgres=> SELECT * FROM leaktest;
ERROR:  permission denied for table leaktest

postgres=> LISTEN pgevents_event;
Asynchronous notification "pgevents_event" with payload
"{"table":"leaktest","action":"INSERT","data":"{\"id\":1,\"secret\":\"...\"}"}" received
```

That is how LISTEN/NOTIFY works, not something this library can grant or deny. What follows from it:

- Treat a table you attach as readable by anyone who may connect to that database.
- Applications sharing a database also receive each other's events, since the channel is one.

For a table whose contents must not travel, attach it without the row:

```golang
listener.AttachWithoutRow("my_table", pgevents.Insert, pgevents.Update)
```

The event then says which table changed and how, `Data` is empty and `Truncated` is set - the same
shape as for a row too large to send. Read the row from the table when you need it, as whatever user
your application connects as.

## Table names

A name that is a valid unquoted SQL identifier is folded to lower case, exactly as
Postgres does, so `Attach("MyTable")` refers to `mytable`. Names that need quoting,
such as ones with spaces or reserved words like `user`, work as given. To refer to a
case-sensitive table, pass the name in double quotes: `Attach("\"MyTable\"")`.

A name may name its schema: `Attach("reporting.events")`, and each part is quoted on its
own. A dot inside a quoted name stays part of the name, so `Attach("\"reports.2026\"")`
refers to one table.

## Connections

The listener keeps one connection of its own for `LISTEN`, because a connection
that listens cannot be shared or handed back to a pool. When it breaks - a server
restart, a network that drops the flow - the listener reconnects by itself, waiting
longer between attempts up to a minute, and then calls the `OnReconnect` callbacks:
notifications sent while there was no connection are gone, so whatever keeps a copy
of the data has to read it again. A connection nothing arrives on is checked once a
minute, so one that went away silently is noticed rather than waited on forever.

`OpenListener` connects before it returns, so a database that cannot be reached is
an error you get there and not a line in the log every few seconds.

## Running several applications against one database

`OpenListener` and `Attach` install their function and trigger under an advisory
lock, so several instances starting at the same time do not fail on each other's
changes. Applications using an older pg-events version do not take that lock; during
a rolling upgrade from one, a start can still fail once and succeed on retry.

Calling `OnEvent`, `OnReconnect` and `Close` concurrently with event delivery is
safe, and `Close` may be called more than once. Do not call `Close` from inside a
callback: it waits for running callbacks to return.

A callback that panics is logged with its stack and the listener carries on, so one
broken handler does not leave the application connected and silent.

## Tests

The tests need a real Postgres. `run-database.sh` starts one in Docker and prints
the command to run them.
