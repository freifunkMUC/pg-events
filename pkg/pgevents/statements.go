package pgevents

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"

	"github.com/lib/pq"
)

// setupLockKey is the advisory lock that serializes installing the notify
// function and triggers. Postgres cannot run CREATE OR REPLACE FUNCTION for
// the same function concurrently: it fails with "duplicate key value violates
// unique constraint" while the function does not exist yet and with "tuple
// concurrently updated" afterwards. Several applications starting at once hit
// exactly that.
var setupLockKey = func() int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("pg-events/setup"))
	return int64(h.Sum64())
}()

// notifyPayloadLimit is the largest NOTIFY payload Postgres accepts; 8000 bytes
// and more fail with "payload string too long".
const notifyPayloadLimit = 7999

func procedure() string {
	return fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION pgevents_notify_event() RETURNS TRIGGER AS $$

		DECLARE
				data json;
				notification text;

		BEGIN

				-- Convert the old or new row to JSON, based on the kind of action.
				-- Action = DELETE?             -> OLD row
				-- Action = INSERT or UPDATE?   -> NEW row
				IF (TG_OP = 'DELETE') THEN
						data = row_to_json(OLD);
				ELSE
						data = row_to_json(NEW);
				END IF;

				-- Contruct the notification as a JSON string.
				notification = json_build_object(
													'table', TG_TABLE_NAME,
													'action', TG_OP,
													'data', data::text)::text;

				-- NOTIFY rejects payloads of 8000 bytes or more, and an error here
				-- aborts the INSERT, UPDATE or DELETE that fired the trigger. Send the
				-- event without the row instead, so the write itself still succeeds.
				IF octet_length(notification) > %d THEN
						notification = json_build_object(
															'table', TG_TABLE_NAME,
															'action', TG_OP,
															'data', '',
															'truncated', true)::text;
				END IF;

				-- Execute pg_notify(channel, notification)
				PERFORM pg_notify('pgevents_event', notification);

				-- Result is ignored since this is an AFTER trigger
				RETURN NULL;
		END;

		$$ LANGUAGE plpgsql;
	`, notifyPayloadLimit)
}

// dropTrigger and createTrigger (re)install the trigger that reports the given
// actions on table.
func dropTrigger(table string) string {
	return fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", triggerName(table), identifier(table))
}

func createTrigger(table string, actions []Action) string {
	events := make([]string, len(actions))
	for i, action := range actions {
		events[i] = string(action)
	}
	return fmt.Sprintf(
		"CREATE TRIGGER %s AFTER %s ON %s FOR EACH ROW EXECUTE PROCEDURE pgevents_notify_event()",
		triggerName(table), strings.Join(events, " OR "), identifier(table),
	)
}

func triggerName(table string) string {
	return pq.QuoteIdentifier(identifierName(table) + "_events")
}

var unquotedIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

// identifier turns a table name into a SQL identifier. Earlier versions put the
// name into the statement unquoted, so it keeps meaning what it meant then:
// a name that is valid unquoted is folded to lower case exactly as Postgres
// folds it, and a name the caller already put in double quotes is taken
// literally. Everything else - spaces, other characters - is quoted as given,
// which also makes reserved words such as "user" usable.
func identifier(table string) string {
	return pq.QuoteIdentifier(identifierName(table))
}

func identifierName(table string) string {
	if unquotedIdentifier.MatchString(table) {
		return strings.ToLower(table)
	}
	if len(table) >= 2 && strings.HasPrefix(table, `"`) && strings.HasSuffix(table, `"`) {
		return strings.ReplaceAll(table[1:len(table)-1], `""`, `"`)
	}
	return table
}
