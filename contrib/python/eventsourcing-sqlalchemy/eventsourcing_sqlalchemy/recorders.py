# -*- coding: utf-8 -*-
from __future__ import annotations

import select
import time
from threading import Thread
from typing import Any, Callable, List, Optional, Sequence, Type, cast
from uuid import UUID

from eventsourcing.persistence import (
    AggregateRecorder,
    ApplicationRecorder,
    IntegrityError,
    ListenNotifySubscription,
    Notification,
    ProcessRecorder,
    ProgrammingError,
    StoredEvent,
    Subscription,
    Tracking,
    TrackingRecorder,
)
from sqlalchemy import Column, Table, text
from sqlalchemy.orm import Session
from typing_extensions import TypeVar

from eventsourcing_sqlalchemy.datastore import SQLAlchemyDatastore, Transaction
from eventsourcing_sqlalchemy.models import (  # type: ignore
    EventRecord,
    StoredEventRecord,
)


class SQLAlchemyRecorder:
    """Base class for recorders that use SQLAlchemy."""

    POSTGRES_MAX_IDENTIFIER_LEN = 63

    def __init__(
        self,
        datastore: SQLAlchemyDatastore,
        schema_name: str | None = None,
    ):
        self.datastore = datastore
        self.schema_name = schema_name
        self.tables: List[Table] = []

    def check_identifier_length(self, table_name: str) -> None:
        assert self.datastore.engine is not None
        if self.datastore.engine.dialect.name == "postgresql":
            if len(table_name) > SQLAlchemyRecorder.POSTGRES_MAX_IDENTIFIER_LEN:
                msg = f"Identifier too long: {table_name}"
                raise ProgrammingError(msg)

    def create_table(self) -> None:
        assert self.datastore.engine is not None
        for table in self.tables:
            table.create(self.datastore.engine, checkfirst=True)

    def transaction(self, commit: bool = True) -> Transaction:
        return self.datastore.transaction(commit=commit)


class SQLAlchemyAggregateRecorder(SQLAlchemyRecorder, AggregateRecorder):
    def __init__(
        self,
        datastore: SQLAlchemyDatastore,
        *,
        events_table_name: str,
        schema_name: str | None = None,
        for_snapshots: bool = False,
    ):
        super().__init__(
            datastore,
            schema_name=schema_name,
        )
        self.events_table_name = events_table_name
        record_cls_name = "".join(
            [
                s.capitalize()
                for s in (schema_name or "").split("_") + events_table_name.rstrip(
                    "s"
                ).split("_")
            ]
        )
        if not for_snapshots:
            base_cls: Type[EventRecord] = self.datastore.base_stored_event_record_cls
            self._has_autoincrementing_ids = True
        else:
            base_cls = self.datastore.base_snapshot_record_cls
            self._has_autoincrementing_ids = False
        self.events_record_cls = self.datastore.define_record_class(
            cls_name=record_cls_name,
            table_name=self.events_table_name,
            schema_name=self.schema_name,
            base_cls=base_cls,
        )
        self.tables.append(self.events_record_cls.__table__)

    def insert_events(
        self, stored_events: Sequence[StoredEvent], **kwargs: Any
    ) -> Optional[Sequence[int]]:
        with self.transaction(commit=True) as session:
            self._insert_stored_events(session, stored_events, **kwargs)
        return None

    def _insert_stored_events(
        self, session: Session, stored_events: Sequence[StoredEvent], **kwargs: Any
    ) -> Optional[Sequence[int]]:
        records = [
            self.events_record_cls(
                originator_id=e.originator_id,
                originator_version=e.originator_version,
                topic=e.topic,
                state=e.state,
            )
            for e in stored_events
        ]
        if self._has_autoincrementing_ids:
            self._lock_table(session)
        for record in records:
            session.add(record)
        if self._has_autoincrementing_ids:
            session.flush()  # We want the autoincremented IDs now.
        return None

    def _lock_table(self, session: Session) -> None:
        assert self.datastore.engine is not None
        events_table_name = self.events_table_name
        if self.schema_name is not None:
            events_table_name = f"{self.schema_name}.{events_table_name}"
        if self.datastore.engine.dialect.name == "postgresql":
            # Todo: "SET LOCAL lock_timeout = '{x}s'" like in eventsourcing.postgres?
            session.execute(text(f"LOCK TABLE {events_table_name} IN EXCLUSIVE MODE"))
        elif self.datastore.engine.dialect.name == "mssql":
            pass
            # This doesn't work to ensure insert and commit order are the same:
            # session.connection(execution_options={"isolation_level": "SERIALIZABLE"})
            # This avoids deadlocks from TABLOCK but together still doesn't ensure
            # insert and commit order are the same:
            # session.execute(text(f"SET LOCK_TIMEOUT 18;"))
            # This gives deadlocks:
            # session.execute(
            #     text(
            #         f"DECLARE  @HideSelectFromOutput TABLE ( DoNotOutput INT); "  # noqa E702
            #         f"INSERT INTO @HideSelectFromOutput "
            #         f"SELECT TOP 1 Id FROM {events_table_name} "
            #         f"WITH (TABLOCK);"  # noqa E231
            #     )
            # )

    def select_events(
        self,
        originator_id: UUID | str,
        gt: Optional[int] = None,
        lte: Optional[int] = None,
        desc: bool = False,
        limit: Optional[int] = None,
    ) -> List[StoredEvent]:
        with self.transaction(commit=False) as session:
            q = session.query(self.events_record_cls)
            q = q.filter(self.events_record_cls.originator_id == originator_id)
            originator_version: Column[int] = self.events_record_cls.originator_version
            if gt is not None:
                q = q.filter(originator_version > gt)
            if lte is not None:
                q = q.filter(originator_version <= lte)
            if desc:
                q = q.order_by(originator_version.desc())
            else:
                q = q.order_by(originator_version)
            if limit is not None:
                records = q[0:limit]
            else:
                records = list(q)

            stored_events = [
                StoredEvent(
                    originator_id=r.originator_id,
                    originator_version=r.originator_version,
                    topic=r.topic,
                    state=(
                        bytes(r.state) if isinstance(r.state, memoryview) else r.state
                    ),
                )
                for r in records
            ]
        return stored_events


class SQLAlchemyApplicationRecorder(SQLAlchemyAggregateRecorder, ApplicationRecorder):
    def __init__(
        self,
        datastore: SQLAlchemyDatastore,
        *,
        events_table_name: str,
        schema_name: str | None = None,
    ):
        super().__init__(
            datastore, events_table_name=events_table_name, schema_name=schema_name
        )
        self.channel_name = self.events_table_name.replace(".", "_")

    def insert_events(
        self,
        stored_events: Sequence[StoredEvent],
        *,
        session: Optional[Session] = None,
        **kwargs: Any,
    ) -> Optional[Sequence[int]]:
        if session is not None:
            assert isinstance(session, Session), type(session)
            self._insert_events(session, stored_events, **kwargs)
            notification_ids = self._insert_stored_events(
                session, stored_events, **kwargs
            )
        else:
            with self.transaction(commit=True) as session:
                self._insert_events(session, stored_events, **kwargs)
                notification_ids = self._insert_stored_events(
                    session, stored_events, **kwargs
                )
        return notification_ids

    def _insert_events(
        self,
        session: Session,
        stored_events: Sequence[StoredEvent],
        **_: Any,
    ) -> Optional[Sequence[int]]:
        pass

    def _insert_stored_events(
        self, session: Session, stored_events: Sequence[StoredEvent], **kwargs: Any
    ) -> Sequence[int]:
        records = [
            self.events_record_cls(
                originator_id=e.originator_id,
                originator_version=e.originator_version,
                topic=e.topic,
                state=e.state,
            )
            for e in stored_events
        ]
        if self._has_autoincrementing_ids:
            self._lock_table(session)
        for record in records:
            session.add(record)
        if self._has_autoincrementing_ids:
            session.flush()  # We want the autoincremented IDs now.
        self._notify_channel(session)
        return [cast(StoredEventRecord, r).id for r in records]

    def max_notification_id(self) -> int | None:
        try:
            with self.transaction(commit=False) as session:
                record_class = self.events_record_cls
                q = session.query(record_class)
                q = q.order_by(record_class.id.desc())
                records = q[0:1]
                return records[0].id
        except (IndexError, AssertionError):
            return None

    def select_notifications(
        self,
        start: int | None,
        limit: int,
        stop: int | None = None,
        topics: Sequence[str] = (),
        *,
        inclusive_of_start: bool = True,
    ) -> list[Notification]:
        with self.transaction(commit=False) as session:
            record_class = self.events_record_cls
            q = session.query(record_class)
            if start is not None:
                if inclusive_of_start:
                    q = q.filter(record_class.id >= start)
                else:
                    q = q.filter(record_class.id > start)
            if stop is not None:
                q = q.filter(record_class.id <= stop)
            if topics:
                q = q.filter(record_class.topic.in_(topics))
            q = q.order_by(record_class.id)  # Make it an index scan
            q = q[0:limit]

            notifications = [
                Notification(
                    id=r.id,
                    originator_id=r.originator_id,
                    originator_version=r.originator_version,
                    topic=r.topic,
                    state=(
                        bytes(r.state) if isinstance(r.state, memoryview) else r.state
                    ),
                )
                for r in q
            ]
        return notifications

    def subscribe(
        self, gt: int | None = None, topics: Sequence[str] = ()
    ) -> Subscription[ApplicationRecorder]:
        assert self.datastore.engine
        if self.datastore.engine.dialect.name == "postgresql":
            return SQLAlchemySubscription(recorder=self, gt=gt, topics=topics)
        else:
            msg = "SQLAlchemyApplicationRecorder.subscribe() is not implemented for"
            msg += f"{self.datastore.engine.dialect}"
            raise NotImplementedError(msg)

    def _notify_channel(self, session: Session) -> None:
        """
        Send a NOTIFY on the channel using a SQLAlchemy connection.
        """
        assert self.datastore.engine
        if self.datastore.engine.dialect.name == "postgresql":
            # Get the raw psycopg connection
            cursor = session.connection().connection.cursor()
            cursor.execute(f"NOTIFY {self.channel_name};")


class SQLAlchemySubscription(ListenNotifySubscription[SQLAlchemyApplicationRecorder]):
    def __init__(
        self,
        recorder: SQLAlchemyApplicationRecorder,
        gt: int | None = None,
        topics: Sequence[str] = (),
    ) -> None:
        assert isinstance(recorder, SQLAlchemyApplicationRecorder)
        super().__init__(recorder=recorder, gt=gt, topics=topics)
        self._listen_thread = Thread(target=self._listen)
        self._listen_thread.start()

    def __exit__(self, *args: object, **kwargs: Any) -> None:
        super().__exit__(*args, **kwargs)
        self._listen_thread.join()

    def _listen(self) -> None:
        assert self._recorder.datastore.engine
        assert self._recorder.datastore.engine.dialect.name == "postgresql"
        notification_handler = self.__get_notification_handler()

        try:
            with self._recorder.datastore.get_connection() as sa_conn:
                sa_conn.execution_options(isolation_level="AUTOCOMMIT")
                raw_conn = sa_conn.connection

                cur = raw_conn.cursor()
                cur.execute(f"LISTEN {self._recorder.channel_name};")

                while not self._has_been_stopped and not self._thread_error:
                    if select.select([raw_conn], [], [], 0.1)[0]:
                        notification_handler(raw_conn)
                    else:
                        time.sleep(0.1)

        except BaseException as e:  # noqa: B036
            if self._thread_error is None:
                self._thread_error = e
            self.stop()

    def __get_notification_handler(self) -> Callable[[Any], None]:
        assert self._recorder.datastore.engine
        driver_name = self._recorder.datastore.engine.dialect.driver
        handlers = {
            "psycopg": self.__handle_psycopg_notification,
            "psycopg2": self.__handle_psycopg2_notification,
        }
        try:
            return handlers[driver_name]
        except KeyError as e:
            raise NotImplementedError(f"Unsupported driver: {driver_name}") from e

    def __handle_psycopg_notification(self, raw_conn: Any) -> None:
        next(raw_conn.notifies())
        self._has_been_notified.set()

    def __handle_psycopg2_notification(self, raw_conn: Any) -> None:
        raw_conn.poll()
        if raw_conn.notifies:
            raw_conn.notifies.pop(0)
            self._has_been_notified.set()


class SQLAlchemyTrackingRecorder(SQLAlchemyRecorder, TrackingRecorder):
    def __init__(
        self,
        datastore: SQLAlchemyDatastore,
        *,
        tracking_table_name: str = "notification_tracking",
        schema_name: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(datastore=datastore, **kwargs)
        self.tracking_table_name = tracking_table_name
        self.tracking_record_cls = self.datastore.define_record_class(
            cls_name="NotificationTrackingRecord",
            table_name=self.tracking_table_name,
            schema_name=schema_name,
            base_cls=datastore.base_notification_tracking_record_cls,
        )
        self.tracking_table: Table = self.tracking_record_cls.__table__

    def create_table(self) -> None:
        super().create_table()
        assert self.datastore.engine is not None
        self.tracking_table.create(self.datastore.engine, checkfirst=True)

    def max_tracking_id(self, application_name: str) -> int | None:
        with self.transaction(commit=False) as session:
            q = session.query(self.tracking_record_cls)
            q = q.filter(self.tracking_record_cls.application_name == application_name)
            q = q.order_by(self.tracking_record_cls.notification_id.desc())
            try:
                max_id = q[0].notification_id
            except IndexError:
                max_id = None
        return max_id

    def insert_tracking(self, tracking: Tracking) -> None:
        with self.transaction(commit=True) as session:
            self._insert_tracking(session=session, tracking=tracking)

    def _insert_tracking(self, session: Session, tracking: Tracking) -> None:
        if tracking is not None:
            if self.has_tracking_id(
                tracking.application_name, tracking.notification_id
            ):
                raise IntegrityError

        existing = (
            session.query(self.tracking_record_cls)
            .filter_by(application_name=str(tracking.application_name))
            .first()
        )
        if existing:
            existing.notification_id = tracking.notification_id
        else:
            record = self.tracking_record_cls(
                application_name=tracking.application_name,
                notification_id=tracking.notification_id,
            )
            session.add(record)


TSQLAlchemyTrackingRecorder = TypeVar(
    "TSQLAlchemyTrackingRecorder",
    bound=SQLAlchemyTrackingRecorder,
    default=SQLAlchemyTrackingRecorder,
)


class SQLAlchemyProcessRecorder(
    SQLAlchemyTrackingRecorder, SQLAlchemyApplicationRecorder, ProcessRecorder
):
    def __init__(
        self,
        datastore: SQLAlchemyDatastore,
        *,
        events_table_name: str,
        tracking_table_name: str,
        schema_name: str | None = None,
    ):
        super().__init__(
            datastore=datastore,
            tracking_table_name=tracking_table_name,
            events_table_name=events_table_name,
            schema_name=schema_name,
        )

    def _insert_events(
        self, session: Session, stored_events: Sequence[StoredEvent], **kwargs: Any
    ) -> None:
        tracking: Optional[Tracking] = kwargs.get("tracking", None)
        if tracking is not None:
            self._insert_tracking(session, tracking)
        super()._insert_events(session=session, stored_events=stored_events, **kwargs)
