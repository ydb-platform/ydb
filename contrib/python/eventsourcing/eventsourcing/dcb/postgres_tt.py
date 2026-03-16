from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any, NamedTuple

from psycopg.generators import notifies
from psycopg.sql import SQL, Composed, Identifier

from eventsourcing.dcb.api import (
    DCBAppendCondition,
    DCBEvent,
    DCBQuery,
    DCBQueryItem,
    DCBReadResponse,
    DCBRecorder,
    DCBSequencedEvent,
)
from eventsourcing.dcb.persistence import (
    DCBInfrastructureFactory,
    DCBListenNotifySubscription,
)
from eventsourcing.dcb.popo import SimpleDCBReadResponse
from eventsourcing.persistence import IntegrityError, InternalError, ProgrammingError
from eventsourcing.postgres import (
    NO_TRACEBACK,
    BasePostgresFactory,
    PostgresDatastore,
    PostgresRecorder,
    PostgresTrackingRecorder,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from psycopg import Connection, Cursor
    from psycopg.abc import Params
    from psycopg.rows import DictRow

DB_TYPE_NAME_DCB_EVENT_TT = "dcb_event_tt"

DB_TYPE_DCB_EVENT = SQL("""
CREATE TYPE {schema}.{event_type} AS (
    type text,
    data bytea,
    tags text[]
)
""")

DB_TYPE_NAME_DCB_QUERY_ITEM_TT = "dcb_query_item_tt"

DB_TYPE_DCB_QUERY_ITEM = SQL("""
CREATE TYPE {schema}.{query_item_type} AS (
    types text[],
    tags text[]
)
""")

DB_TABLE_DCB_EVENTS = SQL("""
CREATE TABLE IF NOT EXISTS {schema}.{events_table} (
    id bigserial,
    type text NOT NULL ,
    data bytea,
    tags text[] NOT NULL
) WITH (
  autovacuum_enabled = true,
  autovacuum_vacuum_threshold = 100000000,  -- Effectively disables VACUUM
  autovacuum_vacuum_scale_factor = 0.5,     -- Same here, high scale factor
  autovacuum_analyze_threshold = 1000,      -- Triggers ANALYZE more often
  autovacuum_analyze_scale_factor = 0.01    -- Triggers after 1% new rows
)
""")

DB_INDEX_UNIQUE_ID_COVER_TYPE = SQL("""
CREATE UNIQUE INDEX IF NOT EXISTS {id_cover_type_index} ON
{schema}.{events_table} (id) INCLUDE (type)
""")

DB_TABLE_DCB_TAGS = SQL("""
CREATE TABLE IF NOT EXISTS {schema}.{tags_table} (
    tag text,
    main_id bigint REFERENCES {events_table} (id)
) WITH (
    autovacuum_enabled = true,
    autovacuum_vacuum_threshold = 100000000,  -- Effectively disables VACUUM
    autovacuum_vacuum_scale_factor = 0.5,     -- Same here, high scale factor
    autovacuum_analyze_threshold = 1000,      -- Triggers ANALYZE more often
    autovacuum_analyze_scale_factor = 0.01    -- Triggers after 1% new rows
)
""")

DB_INDEX_TAG_MAIN_ID = SQL("""
CREATE INDEX IF NOT EXISTS {tag_main_id_index} ON
{schema}.{tags_table} (tag, main_id)
""")

SQL_SELECT_ALL = SQL("""
SELECT * FROM {schema}.{events_table}
WHERE id > COALESCE(%(after)s, 0)
ORDER BY id ASC
LIMIT COALESCE(%(limit)s, 9223372036854775807)
""")

SQL_SELECT_EVENTS_BY_TYPE = SQL("""
SELECT * FROM {schema}.{events_table}
WHERE type = %(event_type)s
AND id > COALESCE(%(after)s, 0)
ORDER BY id ASC
LIMIT COALESCE(%(limit)s, 9223372036854775807)
""")

SQL_SELECT_MAX_ID = SQL("""
SELECT MAX(id) FROM {schema}.{events_table}
""")

SQL_SELECT_BY_TAGS = SQL("""
WITH query_items AS (
    SELECT * FROM unnest(
        %(query_items)s::{schema}.{query_item_type}[]
    ) WITH ORDINALITY
),
initial_matches AS (
    SELECT
        t.main_id,
        qi.ordinality,
        t.tag,
        qi.tags AS required_tags,
        qi.types AS allowed_types
    FROM query_items qi
    JOIN {schema}.{tags_table} t
      ON t.tag = ANY(qi.tags)
   WHERE t.main_id > COALESCE(%(after)s, 0)
),
matched_groups AS (
    SELECT
        main_id,
        ordinality,
        COUNT(DISTINCT tag) AS matched_tag_count,
        array_length(required_tags, 1) AS required_tag_count,
        allowed_types
    FROM initial_matches
    GROUP BY main_id, ordinality, required_tag_count, allowed_types
),
qualified_ids AS (
    SELECT main_id, allowed_types
    FROM matched_groups
    WHERE matched_tag_count = required_tag_count
),
filtered_ids AS (
    SELECT m.id
    FROM {schema}.{events_table} m
    JOIN qualified_ids q ON q.main_id = m.id
    WHERE
        m.id > COALESCE(%(after)s, 0)
        AND (
            array_length(q.allowed_types, 1) IS NULL
            OR array_length(q.allowed_types, 1) = 0
            OR m.type = ANY(q.allowed_types)
        )
    ORDER BY m.id ASC
    LIMIT COALESCE(%(limit)s, 9223372036854775807)
)
SELECT *
FROM {schema}.{events_table}  m
WHERE m.id IN (SELECT id FROM filtered_ids)
ORDER BY m.id ASC;
""")


SQL_UNCONDITIONAL_APPEND = SQL("""
SELECT * FROM {schema}.{unconditional_append}(%(events)s)
""")
DB_FUNCTION_NAME_DCB_UNCONDITIONAL_APPEND_TT = "dcb_unconditional_append_tt"
DB_FUNCTION_UNCONDITIONAL_APPEND = SQL("""
CREATE OR REPLACE FUNCTION {schema}.{unconditional_append}(
    new_events {schema}.{event_type}[]
) RETURNS SETOF bigint
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    WITH new_data AS (
        SELECT * FROM unnest(new_events)
    ),
    inserted AS (
        INSERT INTO {schema}.{events_table} (type, data, tags)
        SELECT type, data, tags
        FROM new_data
        RETURNING id, tags
    ),
    expanded_tags AS (
        SELECT ins.id AS main_id, tag
        FROM inserted ins,
             unnest(ins.tags) AS tag
    ),
    tag_insert AS (
        INSERT INTO {schema}.{tags_table} (tag, main_id)
        SELECT tag, main_id
        FROM expanded_tags
    )
    SELECT MAX(id) FROM inserted;
    NOTIFY {channel};

END
$$;
""")

SQL_CONDITIONAL_APPEND = SQL("""
SELECT * FROM {schema}.{conditional_append}(%(query_items)s, %(after)s, %(events)s)
""")
DB_FUNCTION_NAME_DCB_CONDITIONAL_APPEND_TT = "dcb_conditional_append_tt"
DB_FUNCTION_CONDITIONAL_APPEND = SQL("""
CREATE OR REPLACE FUNCTION {schema}.{conditional_append}(
    query_items {schema}.{query_item_type}[],
    after_id bigint,
    new_events {schema}.{event_type}[]
) RETURNS SETOF bigint
LANGUAGE plpgsql AS $$
DECLARE
    conflict_exists boolean;
BEGIN
    -- Step 0: Lock table in exclusive mode (reads can still read)
    SET LOCAL lock_timeout = '{lock_timeout}s';
    LOCK TABLE {schema}.{events_table} IN EXCLUSIVE MODE;

    -- Step 1: Check for conflicts
    WITH query_items_cte AS (
        SELECT * FROM unnest(query_items) WITH ORDINALITY
    ),
    initial_matches AS (
        SELECT
            t.main_id,
            qi.ordinality,
            t.tag,
            qi.tags AS required_tags,
            qi.types AS allowed_types
        FROM query_items_cte qi
        JOIN {schema}.{tags_table} t
          ON t.tag = ANY(qi.tags)
        WHERE t.main_id > COALESCE(after_id, 0)
    ),
    matched_groups AS (
        SELECT
            main_id,
            ordinality,
            COUNT(DISTINCT tag) AS matched_tag_count,
            array_length(required_tags, 1) AS required_tag_count,
            allowed_types
        FROM initial_matches
        GROUP BY main_id, ordinality, required_tag_count, allowed_types
    ),
    qualified_ids AS (
        SELECT main_id, allowed_types
        FROM matched_groups
        WHERE matched_tag_count = required_tag_count
    ),
    filtered_ids AS (
        SELECT m.id
        FROM {schema}.{events_table} m
        JOIN qualified_ids q ON q.main_id = m.id
        WHERE
            m.id > COALESCE(after_id, 0)
            AND (
                array_length(q.allowed_types, 1) IS NULL
                OR array_length(q.allowed_types, 1) = 0
                OR m.type = ANY(q.allowed_types)
            )
        LIMIT 1
    )
    SELECT EXISTS (SELECT 1 FROM filtered_ids)
    INTO conflict_exists;

    -- Step 2: Insert if no conflicts
    IF NOT conflict_exists THEN
        RETURN QUERY
        WITH new_data AS (
            SELECT * FROM unnest(new_events)
        ),
        inserted AS (
            INSERT INTO {schema}.{events_table} (type, data, tags)
            SELECT type, data, tags
            FROM new_data
            RETURNING id, tags
        ),
        expanded_tags AS (
            SELECT ins.id AS main_id, tag
            FROM inserted ins,
                 unnest(ins.tags) AS tag
        ),
        tag_insert AS (
            INSERT INTO {schema}.{tags_table} (tag, main_id)
            SELECT tag, main_id
            FROM expanded_tags
        )
        SELECT MAX(id) FROM inserted;
        NOTIFY {channel};

    END IF;

    -- If conflict exists, return empty result
    RETURN;
END
$$;
""")


SQL_SET_LOCAL_LOCK_TIMEOUT = SQL("SET LOCAL lock_timeout = '{lock_timeout}s'")
SQL_LOCK_TABLE = SQL("LOCK TABLE {schema}.{events_table} IN EXCLUSIVE MODE")

SQL_EXPLAIN = SQL("EXPLAIN")
SQL_EXPLAIN_ANALYZE = SQL("EXPLAIN (ANALYZE, BUFFERS, VERBOSE)")


class PostgresDCBRecorderTT(DCBRecorder, PostgresRecorder):
    def __init__(
        self,
        datastore: PostgresDatastore,
        *,
        events_table_name: str = "dcb_events",
    ):
        super().__init__(datastore)
        # Define identifiers.
        self.events_table_name = events_table_name + "_tt_main"
        self.channel_name = self.events_table_name.replace(".", "_")
        self.tags_table_name = events_table_name + "_tt_tag"
        self.index_name_id_cover_type = self.events_table_name + "_idx_id_type"
        self.index_name_tag_main_id = self.tags_table_name + "_idx_tag_main_id"

        # Check identifier lengths.
        self.check_identifier_length(self.events_table_name)
        self.check_identifier_length(self.tags_table_name)
        self.check_identifier_length(self.index_name_id_cover_type)
        self.check_identifier_length(self.index_name_tag_main_id)
        self.check_identifier_length(DB_TYPE_NAME_DCB_EVENT_TT)
        self.check_identifier_length(DB_TYPE_NAME_DCB_QUERY_ITEM_TT)

        # Register composite database types.
        self.datastore.db_type_names.add(DB_TYPE_NAME_DCB_EVENT_TT)
        self.datastore.db_type_names.add(DB_TYPE_NAME_DCB_QUERY_ITEM_TT)
        self.datastore.register_type_adapters()

        # Define SQL template keyword arguments.
        self.sql_kwargs = {
            "schema": Identifier(self.datastore.schema),
            "events_table": Identifier(self.events_table_name),
            "channel": Identifier(self.channel_name),
            "tags_table": Identifier(self.tags_table_name),
            "event_type": Identifier(DB_TYPE_NAME_DCB_EVENT_TT),
            "query_item_type": Identifier(DB_TYPE_NAME_DCB_QUERY_ITEM_TT),
            "id_cover_type_index": Identifier(self.index_name_id_cover_type),
            "tag_main_id_index": Identifier(self.index_name_tag_main_id),
            "unconditional_append": Identifier(
                DB_FUNCTION_NAME_DCB_UNCONDITIONAL_APPEND_TT
            ),
            "conditional_append": Identifier(
                DB_FUNCTION_NAME_DCB_CONDITIONAL_APPEND_TT
            ),
            "lock_timeout": self.datastore.lock_timeout,
        }

        # Format and extend SQL create statements.
        self.sql_create_statements.extend(
            [
                self.format(DB_TYPE_DCB_EVENT),
                self.format(DB_TYPE_DCB_QUERY_ITEM),
                self.format(DB_TABLE_DCB_EVENTS),
                self.format(DB_INDEX_UNIQUE_ID_COVER_TYPE),
                self.format(DB_TABLE_DCB_TAGS),
                self.format(DB_INDEX_TAG_MAIN_ID),
                self.format(DB_FUNCTION_UNCONDITIONAL_APPEND),
                self.format(DB_FUNCTION_CONDITIONAL_APPEND),
            ]
        )

        # Format other SQL statements.
        self.sql_select_by_tags = self.format(SQL_SELECT_BY_TAGS)
        self.sql_select_all = self.format(SQL_SELECT_ALL)
        self.sql_select_by_type = self.format(SQL_SELECT_EVENTS_BY_TYPE)
        self.sql_select_max_id = self.format(SQL_SELECT_MAX_ID)
        self.sql_unconditional_append = self.format(SQL_UNCONDITIONAL_APPEND)
        self.sql_conditional_append = self.format(SQL_CONDITIONAL_APPEND)
        self.sql_set_local_lock_timeout = self.format(SQL_SET_LOCAL_LOCK_TIMEOUT)
        self.sql_lock_table = self.format(SQL_LOCK_TABLE)

    def format(self, sql: SQL) -> Composed:
        return sql.format(**self.sql_kwargs)

    def read(
        self,
        query: DCBQuery | None = None,
        *,
        after: int | None = None,
        limit: int | None = None,
    ) -> DCBReadResponse:
        with self.datastore.cursor() as curs:
            events, head = self._read(
                curs=curs,
                query=query,
                after=after,
                limit=limit,
                return_head=True,
            )
            # TODO: Actually return an iterator from _read()!
            return SimpleDCBReadResponse(events=iter(events), head=head)

    def _read(
        self,
        curs: Cursor[DictRow],
        query: DCBQuery | None = None,
        *,
        after: int | None = None,
        limit: int | None = None,
        return_head: bool = True,
    ) -> tuple[Sequence[DCBSequencedEvent], int | None]:
        if return_head and limit is None:
            self.execute(curs, self.sql_select_max_id, explain=False)
            row = curs.fetchone()
            head = None if row is None else row["max"]
        else:
            head = None

        if not query or not query.items:
            # Select all.
            self.execute(
                curs,
                self.sql_select_all,
                {
                    "after": after,
                    "limit": limit,
                },
                explain=False,
            )
            rows = curs.fetchall()

        elif self.all_query_items_have_tags(query):
            # Select with tags.
            psycopg_dcb_query_items = self.construct_psycopg_query_items(query.items)

            self.execute(
                curs,
                self.sql_select_by_tags,
                {
                    "query_items": psycopg_dcb_query_items,
                    "after": after,
                    "limit": limit,
                },
                explain=False,
            )
            rows = curs.fetchall()

        elif self.has_one_query_item_one_type(query):
            # Select for one type.
            self.execute(
                curs,
                self.sql_select_by_type,
                {
                    "event_type": query.items[0].types[0],
                    "after": after,
                    "limit": limit,
                },
                explain=False,
            )
            rows = curs.fetchall()

        else:
            msg = f"Unsupported query: {query}"
            raise ProgrammingError(msg)

        events = [
            DCBSequencedEvent(
                event=DCBEvent(
                    type=row["type"],
                    data=row["data"],
                    tags=row["tags"],
                ),
                position=row["id"],
            )
            for row in rows
        ]

        # Maybe update head.
        if return_head and events:
            head = max(head or 0, *[e.position for e in events])

        return events, head

    def subscribe(
        self,
        query: DCBQuery | None = None,
        *,
        after: int | None = None,
    ) -> PostgresDCBSubscription:
        return PostgresDCBSubscription(
            recorder=self,
            query=query,
            after=after,
        )

    def append(
        self, events: Sequence[DCBEvent], condition: DCBAppendCondition | None = None
    ) -> int:
        assert len(events) > 0
        psycopg_dcb_events = self.construct_psycopg_dcb_events(events)

        # Do single-statement "unconditional append".
        if condition is None:
            with self.datastore.cursor() as curs:
                return self._unconditional_append(curs, psycopg_dcb_events)

        if self.all_query_items_have_tags(condition.fail_if_events_match):
            # Do single-statement "conditional append".
            psycopg_dcb_query_items = self.construct_psycopg_query_items(
                condition.fail_if_events_match.items
            )
            with self.datastore.cursor() as curs:
                self.execute(
                    curs,
                    self.sql_conditional_append,
                    {
                        "query_items": psycopg_dcb_query_items,
                        "after": condition.after,
                        "events": psycopg_dcb_events,
                    },
                    explain=False,
                )
                row = curs.fetchone()
                if row is None:
                    raise IntegrityError

                return row[DB_FUNCTION_NAME_DCB_CONDITIONAL_APPEND_TT]

        # Do separate "read" and "append" operations in a transaction.
        with self.datastore.transaction(commit=True) as curs:

            # Lock the table in exclusive mode (readers can still read) to ensure
            # nothing else will execute an append condition statement until after
            # we have finished inserting new events, whilst expecting that others
            # are playing by the same game. By the way, this is how optimistic
            # locking works.
            if self.datastore.lock_timeout:
                curs.execute(self.sql_set_local_lock_timeout)
            curs.execute(self.sql_lock_table)

            # Check the append condition.
            failed, _ = self._read(
                curs=curs,
                query=condition.fail_if_events_match,
                after=condition.after,
                limit=1,
                return_head=False,
            )
            if failed:
                raise IntegrityError(failed)

            # If okay, then do an "unconditional append".
            return self._unconditional_append(curs, psycopg_dcb_events)

    def _unconditional_append(
        self, curs: Cursor[DictRow], psycopg_dcb_events: list[PsycopgDCBEvent]
    ) -> int:
        self.execute(
            curs,
            self.sql_unconditional_append,
            {
                "events": psycopg_dcb_events,
            },
            explain=False,
        )
        row = curs.fetchone()
        if row is None:  # pragma: no cover
            msg = "Shouldn't get here"
            raise InternalError(msg)

        return row[DB_FUNCTION_NAME_DCB_UNCONDITIONAL_APPEND_TT]

    def construct_psycopg_dcb_events(
        self, dcb_events: Sequence[DCBEvent]
    ) -> list[PsycopgDCBEvent]:
        return [
            self.datastore.psycopg_python_types[DB_TYPE_NAME_DCB_EVENT_TT](
                type=e.type,
                data=e.data,
                tags=e.tags,
            )
            for e in dcb_events
        ]

    def construct_psycopg_query_items(
        self, query_items: Sequence[DCBQueryItem]
    ) -> list[PsycopgDCBQueryItem]:
        return [
            self.datastore.psycopg_python_types[DB_TYPE_NAME_DCB_QUERY_ITEM_TT](
                types=q.types,
                tags=q.tags,
            )
            for q in query_items
        ]

    def has_one_query_item_one_type(self, query: DCBQuery) -> bool:
        return (
            len(query.items) == 1
            and len(query.items[0].types) == 1
            and len(query.items[0].tags) == 0
        )

    def all_query_items_have_tags(self, query: DCBQuery) -> bool:
        return all(len(q.tags) > 0 for q in query.items) and len(query.items) > 0

    def execute(
        self,
        cursor: Cursor[DictRow],
        statement: Composed,
        params: Params | None = None,
        *,
        explain: bool = False,
        prepare: bool = True,
    ) -> None:
        if explain:  # pragma: no cover
            print()  # noqa: T201
            print("Statement:", statement.as_string().strip())  # noqa: T201
            print("Params:", params)  # noqa: T201
            print()  # noqa: T201
            # with self.datastore.transaction(commit=False) as explain_cursor:
            #     explain_cursor.execute(SQL_EXPLAIN + statement, params)
            #     rows = explain_cursor.fetchall()
            #     print("\n".join([r["QUERY PLAN"] for r in rows]))  # no qa: T201
            #     print()  # no qa: T201
            # with self.datastore.transaction(commit=False) as explain_cursor:
            cursor.execute(SQL_EXPLAIN + statement, params)
            rows = cursor.fetchall()
            print("\n".join([r["QUERY PLAN"] for r in rows]))  # noqa: T201
            print()  # noqa: T201
        cursor.execute(statement, params, prepare=prepare)


class PsycopgDCBEvent(NamedTuple):
    type: str
    data: bytes
    tags: list[str]


class PsycopgDCBQueryItem(NamedTuple):
    types: list[str]
    tags: list[str]


class PostgresDCBSubscription(DCBListenNotifySubscription[PostgresDCBRecorderTT]):
    def __init__(
        self,
        recorder: PostgresDCBRecorderTT,
        query: DCBQuery | None = None,
        after: int | None = None,
    ) -> None:
        super().__init__(recorder=recorder, query=query, after=after)
        self._has_listen_connection = threading.Event()
        self._listen_connection: Connection[dict[str, Any]] | None = None
        self._listen_thread = threading.Thread(target=self._listen, daemon=True)
        self._listen_thread.start()

    def __exit__(self, *args: object, **kwargs: Any) -> None:
        super().__exit__(*args, **kwargs)
        self._listen_thread.join()

    def _listen(self) -> None:
        recorder = self._recorder
        assert isinstance(recorder, PostgresDCBRecorderTT)
        try:
            with recorder.datastore.get_connection() as conn:
                self._listen_connection = conn
                self._has_listen_connection.set()
                conn.execute(
                    SQL("LISTEN {0}").format(Identifier(recorder.channel_name))
                )
                while not self._has_been_stopped and not self._thread_error:
                    # This block simplifies psycopg's conn.notifies(), because
                    # we aren't interested in the actual notify messages, and
                    # also we want to stop consuming notify messages when the
                    # subscription has an error or is otherwise stopped.
                    with conn.lock:
                        try:
                            if conn.wait(notifies(conn.pgconn), interval=1):
                                self._has_been_notified.set()
                        except NO_TRACEBACK as ex:  # pragma: no cover
                            raise ex.with_traceback(None) from None

        except BaseException as e:  # pragma: no cover
            if self._thread_error is None:
                self._thread_error = e
            self.stop()


class PostgresTTDCBFactory(
    BasePostgresFactory[PostgresTrackingRecorder],
    DCBInfrastructureFactory[PostgresTrackingRecorder],
):
    def dcb_recorder(self) -> DCBRecorder:
        prefix = self.env.name.lower() or "dcb"

        dcb_table_name = prefix + "_events"
        recorder = PostgresDCBRecorderTT(
            datastore=self.datastore,
            events_table_name=dcb_table_name,
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder
