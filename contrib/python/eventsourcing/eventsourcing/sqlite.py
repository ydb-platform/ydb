from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import UUID

from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    Connection,
    ConnectionPool,
    Cursor,
    DatabaseError,
    DataError,
    InfrastructureFactory,
    IntegrityError,
    InterfaceError,
    InternalError,
    Notification,
    NotSupportedError,
    OperationalError,
    PersistenceError,
    ProcessRecorder,
    ProgrammingError,
    Recorder,
    StoredEvent,
    Subscription,
    Tracking,
    TrackingRecorder,
)
from eventsourcing.utils import Environment, EnvType, resolve_topic, strtobool

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from types import TracebackType

SQLITE3_DEFAULT_LOCK_TIMEOUT = 5


class SQLiteCursor(Cursor):
    def __init__(self, sqlite_cursor: sqlite3.Cursor):
        self.sqlite_cursor = sqlite_cursor

    def __enter__(self) -> sqlite3.Cursor:
        return self.sqlite_cursor

    def __exit__(self, *args: object, **kwargs: Any) -> None:
        self.sqlite_cursor.close()

    def execute(self, *args: Any, **kwargs: Any) -> None:
        self.sqlite_cursor.execute(*args, **kwargs)

    def executemany(self, *args: Any, **kwargs: Any) -> None:
        self.sqlite_cursor.executemany(*args, **kwargs)

    def fetchall(self) -> Any:
        return self.sqlite_cursor.fetchall()

    def fetchone(self) -> Any:
        return self.sqlite_cursor.fetchone()

    @property
    def lastrowid(self) -> Any:
        return self.sqlite_cursor.lastrowid


class SQLiteConnection(Connection[SQLiteCursor]):
    def __init__(self, sqlite_conn: sqlite3.Connection, max_age: float | None):
        super().__init__(max_age=max_age)
        self._sqlite_conn = sqlite_conn

    @contextmanager
    def transaction(self, *, commit: bool) -> Iterator[SQLiteCursor]:
        # Context managed cursor, and context managed transaction.
        with SQLiteTransaction(self, commit=commit) as curs, curs:
            yield curs

    def cursor(self) -> SQLiteCursor:
        return SQLiteCursor(self._sqlite_conn.cursor())

    def rollback(self) -> None:
        self._sqlite_conn.rollback()

    def commit(self) -> None:
        self._sqlite_conn.commit()

    def _close(self) -> None:
        self._sqlite_conn.close()
        super()._close()


class SQLiteTransaction:
    def __init__(self, connection: SQLiteConnection, *, commit: bool = False):
        self.connection = connection
        self.commit = commit

    def __enter__(self) -> SQLiteCursor:
        # We must issue a "BEGIN" explicitly
        # when running in auto-commit mode.
        cursor = self.connection.cursor()
        cursor.execute("BEGIN")
        return cursor

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        try:
            if exc_val:
                # Roll back all changes
                # if an exception occurs.
                self.connection.rollback()
                raise exc_val
            if not self.commit:
                self.connection.rollback()
            else:
                self.connection.commit()
        except sqlite3.InterfaceError as e:
            raise InterfaceError(e) from e
        except sqlite3.DataError as e:
            raise DataError(e) from e
        except sqlite3.OperationalError as e:
            raise OperationalError(e) from e
        except sqlite3.IntegrityError as e:
            raise IntegrityError(e) from e
        except sqlite3.InternalError as e:
            raise InternalError(e) from e
        except sqlite3.ProgrammingError as e:
            raise ProgrammingError(e) from e
        except sqlite3.NotSupportedError as e:
            raise NotSupportedError(e) from e
        except sqlite3.DatabaseError as e:
            raise DatabaseError(e) from e
        except sqlite3.Error as e:
            raise PersistenceError(e) from e


class SQLiteConnectionPool(ConnectionPool[SQLiteConnection]):
    def __init__(
        self,
        *,
        db_name: str,
        lock_timeout: int | None = None,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: float = 5.0,
        max_age: float | None = None,
        pre_ping: bool = False,
    ):
        self.db_name = db_name
        self.lock_timeout = lock_timeout
        self.is_sqlite_memory_mode = self.detect_memory_mode(db_name)
        self.is_journal_mode_wal = False
        self.journal_mode_was_changed_to_wal = False
        super().__init__(
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            max_age=max_age,
            pre_ping=pre_ping,
            mutually_exclusive_read_write=self.is_sqlite_memory_mode,
        )

    @staticmethod
    def detect_memory_mode(db_name: str) -> bool:
        return bool(db_name) and (":memory:" in db_name or "mode=memory" in db_name)

    def _create_connection(self) -> SQLiteConnection:
        # Make a connection to an SQLite database.
        try:
            c = sqlite3.connect(
                database=self.db_name,
                uri=True,
                check_same_thread=False,
                isolation_level=None,  # Auto-commit mode.
                cached_statements=True,
                timeout=self.lock_timeout or SQLITE3_DEFAULT_LOCK_TIMEOUT,
            )
        except (sqlite3.Error, TypeError) as e:
            raise InterfaceError(e) from e

        # Use WAL (write-ahead log) mode if file-based database.
        if not self.is_sqlite_memory_mode and not self.is_journal_mode_wal:
            cursor = c.cursor()
            cursor.execute("PRAGMA journal_mode;")
            mode = cursor.fetchone()[0]
            if mode.lower() == "wal":
                self.is_journal_mode_wal = True
            else:
                cursor.execute("PRAGMA journal_mode=WAL;")
                self.is_journal_mode_wal = True
                self.journal_mode_was_changed_to_wal = True

        # Set the row factory.
        c.row_factory = sqlite3.Row

        # Return the connection.
        return SQLiteConnection(sqlite_conn=c, max_age=self.max_age)


class SQLiteDatastore:
    def __init__(
        self,
        db_name: str,
        *,
        lock_timeout: int | None = None,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: float = 5.0,
        max_age: float | None = None,
        pre_ping: bool = False,
        single_row_tracking: bool = True,
        originator_id_type: Literal["uuid", "text"] = "uuid",
    ):
        self.pool = SQLiteConnectionPool(
            db_name=db_name,
            lock_timeout=lock_timeout,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            max_age=max_age,
            pre_ping=pre_ping,
        )
        self.single_row_tracking = single_row_tracking
        self.originator_id_type = originator_id_type

    @contextmanager
    def transaction(self, *, commit: bool) -> Iterator[SQLiteCursor]:
        connection = self.get_connection(commit=commit)
        with connection as conn, conn.transaction(commit=commit) as curs:
            yield curs

    @contextmanager
    def get_connection(self, *, commit: bool) -> Iterator[SQLiteConnection]:
        # Using reader-writer interlocking is necessary for in-memory databases,
        # but also speeds up (and provides "fairness") to file-based databases.
        conn = self.pool.get_connection(is_writer=commit)
        try:
            yield conn
        finally:
            self.pool.put_connection(conn)

    def close(self) -> None:
        self.pool.close()

    def __del__(self) -> None:
        self.close()


class SQLiteRecorder(Recorder):
    def __init__(
        self,
        datastore: SQLiteDatastore,
    ):
        assert isinstance(datastore, SQLiteDatastore)
        self.datastore = datastore
        self.create_table_statements = self.construct_create_table_statements()

    def construct_create_table_statements(self) -> list[str]:
        return []

    def create_table(self) -> None:
        with self.datastore.transaction(commit=True) as c:
            self._create_table(c)

    def _create_table(self, c: SQLiteCursor) -> None:
        for statement in self.create_table_statements:
            c.execute(statement)

    def convert_originator_id(self, originator_id: str) -> UUID | str:
        return (
            UUID(originator_id)
            if self.datastore.originator_id_type == "uuid"
            else originator_id
        )


class SQLiteAggregateRecorder(SQLiteRecorder, AggregateRecorder):
    def __init__(
        self,
        datastore: SQLiteDatastore,
        events_table_name: str = "stored_events",
    ):
        self.events_table_name = events_table_name
        super().__init__(datastore)
        self.insert_events_statement = (
            f"INSERT INTO {self.events_table_name} VALUES (?,?,?,?)"
        )
        self.select_events_statement = (
            f"SELECT * FROM {self.events_table_name} WHERE originator_id=? "
        )

    def construct_create_table_statements(self) -> list[str]:
        statements = super().construct_create_table_statements()
        statements.append(
            "CREATE TABLE IF NOT EXISTS "
            f"{self.events_table_name} ("
            "originator_id TEXT, "
            "originator_version INTEGER, "
            "topic TEXT, "
            "state BLOB, "
            "PRIMARY KEY "
            "(originator_id, originator_version)) "
            "WITHOUT ROWID"
        )
        return statements

    def insert_events(
        self, stored_events: Sequence[StoredEvent], **kwargs: Any
    ) -> Sequence[int] | None:
        with self.datastore.transaction(commit=True) as c:
            return self._insert_events(c, stored_events, **kwargs)

    def _insert_events(
        self,
        c: SQLiteCursor,
        stored_events: Sequence[StoredEvent],
        **_: Any,
    ) -> Sequence[int] | None:
        params = [
            (
                (
                    s.originator_id.hex
                    if isinstance(s.originator_id, UUID)
                    else s.originator_id
                ),
                s.originator_version,
                s.topic,
                s.state,
            )
            for s in stored_events
        ]
        c.executemany(self.insert_events_statement, params)
        return None

    def select_events(
        self,
        originator_id: UUID | str,
        *,
        gt: int | None = None,
        lte: int | None = None,
        desc: bool = False,
        limit: int | None = None,
    ) -> Sequence[StoredEvent]:
        statement = self.select_events_statement
        params: list[Any] = [
            originator_id.hex if isinstance(originator_id, UUID) else originator_id
        ]
        if gt is not None:
            statement += "AND originator_version>? "
            params.append(gt)
        if lte is not None:
            statement += "AND originator_version<=? "
            params.append(lte)
        statement += "ORDER BY originator_version "
        if desc is False:
            statement += "ASC "
        else:
            statement += "DESC "
        if limit is not None:
            statement += "LIMIT ? "
            params.append(limit)
        with self.datastore.transaction(commit=False) as c:
            c.execute(statement, params)
            return [
                StoredEvent(
                    originator_id=self.convert_originator_id(row["originator_id"]),
                    originator_version=row["originator_version"],
                    topic=row["topic"],
                    state=row["state"],
                )
                for row in c.fetchall()
            ]


class SQLiteApplicationRecorder(
    SQLiteAggregateRecorder,
    ApplicationRecorder,
):
    def __init__(
        self,
        datastore: SQLiteDatastore,
        events_table_name: str = "stored_events",
    ):
        super().__init__(datastore, events_table_name)
        self.select_max_notification_id_statement = (
            f"SELECT MAX(rowid) FROM {self.events_table_name}"
        )

    def construct_create_table_statements(self) -> list[str]:
        statement = (
            "CREATE TABLE IF NOT EXISTS "
            f"{self.events_table_name} ("
            "originator_id TEXT, "
            "originator_version INTEGER, "
            "topic TEXT, "
            "state BLOB, "
            "PRIMARY KEY "
            "(originator_id, originator_version))"
        )
        return [statement]

    def _insert_events(
        self,
        c: SQLiteCursor,
        stored_events: Sequence[StoredEvent],
        **_: Any,
    ) -> Sequence[int] | None:
        returning = []
        for s in stored_events:
            c.execute(
                self.insert_events_statement,
                (
                    (
                        s.originator_id.hex
                        if isinstance(s.originator_id, UUID)
                        else s.originator_id
                    ),
                    s.originator_version,
                    s.topic,
                    s.state,
                ),
            )
            returning.append(c.lastrowid)
        return returning

    def select_notifications(
        self,
        start: int | None,
        limit: int,
        stop: int | None = None,
        topics: Sequence[str] = (),
        *,
        inclusive_of_start: bool = True,
    ) -> Sequence[Notification]:
        """Returns a list of event notifications
        from 'start', limited by 'limit'.
        """
        params: list[int | str] = []
        statement = f"SELECT rowid, * FROM {self.events_table_name} "
        has_where = False
        if start is not None:
            has_where = True
            statement += "WHERE "
            params.append(start)
            if inclusive_of_start:
                statement += "rowid>=? "
            else:
                statement += "rowid>? "

        if stop is not None:
            if not has_where:
                has_where = True
                statement += "WHERE "
            else:
                statement += "AND "
            params.append(stop)
            statement += "rowid<=? "

        if topics:
            if not has_where:
                statement += "WHERE "
            else:
                statement += "AND "
            params += list(topics)
            statement += f"topic IN ({','.join('?' * len(topics))}) "

        params.append(limit)
        statement += "ORDER BY rowid LIMIT ?"

        with self.datastore.transaction(commit=False) as c:
            c.execute(statement, params)
            return [
                Notification(
                    id=row["rowid"],
                    originator_id=self.convert_originator_id(row["originator_id"]),
                    originator_version=row["originator_version"],
                    topic=row["topic"],
                    state=row["state"],
                )
                for row in c.fetchall()
            ]

    def max_notification_id(self) -> int:
        """Returns the maximum notification ID."""
        with self.datastore.transaction(commit=False) as c:
            return self._max_notification_id(c)

    def _max_notification_id(self, c: SQLiteCursor) -> int:
        c.execute(self.select_max_notification_id_statement)
        return c.fetchone()[0]

    def subscribe(
        self, gt: int | None = None, topics: Sequence[str] = ()
    ) -> Subscription[ApplicationRecorder]:
        """This method is not implemented on this class."""
        msg = f"The {type(self).__qualname__} recorder does not support subscriptions"
        raise NotImplementedError(msg)


class SQLiteTrackingRecorder(SQLiteRecorder, TrackingRecorder):
    def __init__(
        self,
        datastore: SQLiteDatastore,
        **kwargs: Any,
    ):
        super().__init__(datastore, **kwargs)
        self.tracking_table_exists: bool = False
        self.tracking_migration_previous: int | None = None
        self.tracking_migration_current: int | None = None
        self.table_migration_identifier = "__migration__"
        self.has_checked_for_multi_row_tracking_table: bool = False
        if self.datastore.single_row_tracking:
            self.insert_tracking_statement = (
                "INSERT INTO tracking "
                "VALUES (:application_name, :notification_id) "
                "ON CONFLICT (application_name) DO UPDATE "
                "SET notification_id = :notification_id "
                "WHERE tracking.notification_id < :notification_id "
                "RETURNING notification_id"
            )
        else:
            self.insert_tracking_statement = (
                "INSERT INTO tracking VALUES (:application_name, :notification_id)"
            )
        self.select_max_tracking_id_statement = (
            "SELECT MAX(notification_id) FROM tracking WHERE application_name=?"
        )

    def construct_create_table_statements(self) -> list[str]:
        statements = super().construct_create_table_statements()
        if self.datastore.single_row_tracking:
            statements.append(
                "CREATE TABLE IF NOT EXISTS tracking ("
                "application_name TEXT, "
                "notification_id INTEGER, "
                "PRIMARY KEY "
                "(application_name)) "
                "WITHOUT ROWID"
            )
        else:
            statements.append(
                "CREATE TABLE IF NOT EXISTS tracking ("
                "application_name TEXT, "
                "notification_id INTEGER, "
                "PRIMARY KEY "
                "(application_name, notification_id)) "
                "WITHOUT ROWID"
            )
        return statements

    def create_table(self) -> None:
        # Get the migration version.
        try:
            self.tracking_migration_current = self.tracking_migration_previous = (
                self.max_tracking_id(self.table_migration_identifier)
            )
        except OperationalError:
            pass
        else:
            self.tracking_table_exists = True
        super().create_table()
        if (
            not self.datastore.single_row_tracking
            and self.tracking_migration_current is not None
        ):
            msg = "Can't do multi-row tracking with single-row tracking table"
            raise OperationalError(msg)

    def _create_table(self, c: SQLiteCursor) -> None:
        max_tracking_ids: dict[str, int] = {}
        if (
            self.datastore.single_row_tracking
            and self.tracking_table_exists
            and not self.tracking_migration_previous
        ):
            # Migrate tracking to use single-row per application name.
            # - Get all application names.
            c.execute("SELECT DISTINCT application_name FROM tracking")
            application_names: list[str] = [
                select_row["application_name"] for select_row in c.fetchall()
            ]

            # - Get max tracking ID for each application name.
            for application_name in application_names:
                c.execute(self.select_max_tracking_id_statement, (application_name,))
                max_tracking_id_row = c.fetchone()
                assert max_tracking_id_row is not None
                max_tracking_ids[application_name] = max_tracking_id_row[0]
            # - Rename the table.
            drop_table_statement = "ALTER TABLE tracking RENAME TO old1_tracking"
            c.execute(drop_table_statement)
        # Create the table.
        super()._create_table(c)
        # - Maybe insert migration tracking record and application tracking records.
        if self.datastore.single_row_tracking and (
            not self.tracking_table_exists
            or (self.tracking_table_exists and not self.tracking_migration_previous)
        ):
            # - Assume we just created a table for single-row tracking.
            self._insert_tracking(c, Tracking(self.table_migration_identifier, 1))
            self.tracking_migration_current = 1
            for application_name, max_tracking_id in max_tracking_ids.items():
                self._insert_tracking(c, Tracking(application_name, max_tracking_id))

    def insert_tracking(self, tracking: Tracking) -> None:
        with self.datastore.transaction(commit=True) as c:
            self._insert_tracking(c, tracking)

    def _insert_tracking(
        self,
        c: SQLiteCursor,
        tracking: Tracking,
    ) -> None:
        self._check_has_multi_row_tracking_table(c)

        c.execute(
            self.insert_tracking_statement,
            {
                "application_name": tracking.application_name,
                "notification_id": tracking.notification_id,
            },
        )
        if self.datastore.single_row_tracking:
            fetchone = c.fetchone()
            if fetchone is None:
                msg = (
                    "Failed to record tracking for "
                    f"{tracking.application_name} {tracking.notification_id}"
                )
                raise IntegrityError(msg)

    def _check_has_multi_row_tracking_table(self, c: SQLiteCursor) -> None:
        if (
            not self.datastore.single_row_tracking
            and not self.has_checked_for_multi_row_tracking_table
            and self._max_tracking_id(self.table_migration_identifier, c)
        ):
            msg = "Can't do multi-row tracking with single-row tracking table"
            raise OperationalError(msg)
        self.has_checked_for_multi_row_tracking_table = True

    def max_tracking_id(self, application_name: str) -> int | None:
        with self.datastore.transaction(commit=False) as c:
            return self._max_tracking_id(application_name, c)

    def _max_tracking_id(self, application_name: str, c: SQLiteCursor) -> int | None:
        params = [application_name]
        c.execute(self.select_max_tracking_id_statement, params)
        return c.fetchone()[0]


class SQLiteProcessRecorder(
    SQLiteTrackingRecorder,
    SQLiteApplicationRecorder,
    ProcessRecorder,
):
    def __init__(
        self,
        datastore: SQLiteDatastore,
        *,
        events_table_name: str = "stored_events",
    ):
        super().__init__(datastore, events_table_name=events_table_name)

    def _insert_events(
        self,
        c: SQLiteCursor,
        stored_events: Sequence[StoredEvent],
        **kwargs: Any,
    ) -> Sequence[int] | None:
        returning = super()._insert_events(c, stored_events, **kwargs)
        tracking: Tracking | None = kwargs.get("tracking")
        if tracking is not None:
            self._insert_tracking(c, tracking)
        return returning


class SQLiteFactory(InfrastructureFactory[SQLiteTrackingRecorder]):
    SQLITE_DBNAME = "SQLITE_DBNAME"
    SQLITE_LOCK_TIMEOUT = "SQLITE_LOCK_TIMEOUT"
    SQLITE_SINGLE_ROW_TRACKING = "SINGLE_ROW_TRACKING"
    ORIGINATOR_ID_TYPE = "ORIGINATOR_ID_TYPE"
    CREATE_TABLE = "CREATE_TABLE"

    aggregate_recorder_class = SQLiteAggregateRecorder
    application_recorder_class = SQLiteApplicationRecorder
    tracking_recorder_class = SQLiteTrackingRecorder
    process_recorder_class = SQLiteProcessRecorder

    def __init__(self, env: Environment | EnvType | None):
        super().__init__(env)
        db_name = self.env.get(self.SQLITE_DBNAME)
        if not db_name:
            msg = (
                "SQLite database name not found "
                "in environment with keys: "
                f"{', '.join(self.env.create_keys(self.SQLITE_DBNAME))}"
            )
            raise OSError(msg)

        lock_timeout_str = (
            self.env.get(self.SQLITE_LOCK_TIMEOUT) or ""
        ).strip() or None

        lock_timeout: int | None = None
        if lock_timeout_str is not None:
            try:
                lock_timeout = int(lock_timeout_str)
            except ValueError:
                msg = (
                    "SQLite environment value for key "
                    f"'{self.SQLITE_LOCK_TIMEOUT}' is invalid. "
                    "If set, an int or empty string is expected: "
                    f"'{lock_timeout_str}'"
                )
                raise OSError(msg) from None

        single_row_tracking = strtobool(
            self.env.get(self.SQLITE_SINGLE_ROW_TRACKING, "t")
        )

        originator_id_type = cast(
            Literal["uuid", "text"],
            self.env.get(self.ORIGINATOR_ID_TYPE, "uuid"),
        )
        if originator_id_type.lower() not in ("uuid", "text"):
            msg = (
                f"Invalid {self.ORIGINATOR_ID_TYPE} '{originator_id_type}', "
                f"must be 'uuid' or 'text'"
            )
            raise OSError(msg)

        self.datastore = SQLiteDatastore(
            db_name=db_name,
            lock_timeout=lock_timeout,
            single_row_tracking=single_row_tracking,
            originator_id_type=originator_id_type,
        )

    def aggregate_recorder(self, purpose: str = "events") -> AggregateRecorder:
        events_table_name = "stored_" + purpose
        recorder = self.aggregate_recorder_class(
            datastore=self.datastore,
            events_table_name=events_table_name,
        )
        if self.env_create_table():
            recorder.create_table()
        return recorder

    def application_recorder(self) -> ApplicationRecorder:
        application_recorder_topic = self.env.get(self.APPLICATION_RECORDER_TOPIC)

        if application_recorder_topic:
            application_recorder_class: type[SQLiteApplicationRecorder] = resolve_topic(
                application_recorder_topic
            )
            assert issubclass(application_recorder_class, SQLiteApplicationRecorder)
        else:
            application_recorder_class = self.application_recorder_class

        recorder = application_recorder_class(datastore=self.datastore)

        if self.env_create_table():
            recorder.create_table()
        return recorder

    def tracking_recorder(
        self, tracking_recorder_class: type[SQLiteTrackingRecorder] | None = None
    ) -> SQLiteTrackingRecorder:
        if tracking_recorder_class is None:
            tracking_recorder_topic = self.env.get(self.TRACKING_RECORDER_TOPIC)

            if tracking_recorder_topic:
                tracking_recorder_class = resolve_topic(tracking_recorder_topic)
            else:
                tracking_recorder_class = self.tracking_recorder_class

        assert tracking_recorder_class is not None
        assert issubclass(tracking_recorder_class, SQLiteTrackingRecorder)

        recorder = tracking_recorder_class(datastore=self.datastore)

        if self.env_create_table():
            recorder.create_table()
        return recorder

    def process_recorder(self) -> ProcessRecorder:
        process_recorder_topic = self.env.get(self.PROCESS_RECORDER_TOPIC)

        if process_recorder_topic:
            process_recorder_class: type[SQLiteProcessRecorder] = resolve_topic(
                process_recorder_topic
            )
            assert issubclass(process_recorder_class, SQLiteProcessRecorder)
        else:
            process_recorder_class = self.process_recorder_class

        recorder = process_recorder_class(datastore=self.datastore)

        if self.env_create_table():
            recorder.create_table()
        return recorder

    def env_create_table(self) -> bool:
        default = "yes"
        return bool(strtobool(self.env.get(self.CREATE_TABLE, default) or default))

    def close(self) -> None:
        self.datastore.close()


Factory = SQLiteFactory
