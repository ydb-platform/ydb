# -*- coding: utf-8 -*-
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from contextvars import ContextVar, Token
from threading import Lock, Semaphore
from typing import Any, Dict, Iterator, List, Optional, Tuple, Type, Union, cast

import sqlalchemy.exc
from eventsourcing.persistence import (
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    PersistenceError,
    ProgrammingError,
)
from sqlalchemy import Index, text
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.future import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from sqlalchemy.pool import StaticPool
from typing_extensions import TypeVar

from eventsourcing_sqlalchemy.models import (  # type: ignore
    EventRecord,
    NotificationTrackingRecord,
    SnapshotRecord,
    StoredEventRecord,
)

TEventRecord = TypeVar("TEventRecord", bound=EventRecord)


transactions: ContextVar["Transaction"] = ContextVar("transactions")

exception_classes = {
    ec.__name__: ec
    for ec in (
        DatabaseError,
        DataError,
        IntegrityError,
        InterfaceError,
        InternalError,
        NotSupportedError,
        OperationalError,
        PersistenceError,
        ProgrammingError,
    )
}


class Transaction:
    def __init__(
        self,
        session: Session,
        commit: bool,
        lock: Optional[Semaphore],
        is_scoped_session: bool = False,
    ):
        self.session = session
        self.commit = commit
        self.lock = lock
        self.nested_level = 0
        self.token: Optional[Token["Transaction"]] = None
        self.is_scoped_session = is_scoped_session

    def __enter__(self) -> Session:
        if self.nested_level == 0:
            if not self.is_scoped_session:
                # This doesn't work to ensure commit and insert order are the same:
                # if self.commit:
                #     self.session.execute(text("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"))
                # else:
                #     self.session.begin()
                self.session.begin()
        self.nested_level += 1
        return self.session

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:  # noqa C901
        self.nested_level -= 1
        if self.nested_level == 0:
            try:
                if exc_val:
                    if not self.is_scoped_session:
                        self.session.rollback()
                    raise exc_val
                elif not self.commit:
                    try:
                        if not self.is_scoped_session:
                            self.session.rollback()
                    except sqlite3.OperationalError:
                        pass
                else:
                    if not self.is_scoped_session:
                        self.session.commit()
            except sqlalchemy.exc.InterfaceError as e:
                raise InterfaceError(e) from e
            except sqlalchemy.exc.DataError as e:
                raise DataError(e) from e
            except sqlalchemy.exc.OperationalError as e:
                if isinstance(e.args[0], sqlite3.OperationalError) and self.lock:
                    pass
                else:
                    raise OperationalError(e) from e
            except sqlalchemy.exc.IntegrityError as e:
                raise IntegrityError(e) from e
            except sqlalchemy.exc.InternalError as e:
                raise InternalError(e) from e
            except sqlalchemy.exc.ProgrammingError as e:
                raise ProgrammingError(e) from e
            except sqlalchemy.exc.NotSupportedError as e:
                raise NotSupportedError(e) from e
            except sqlalchemy.exc.DatabaseError as e:
                raise DatabaseError(e) from e
            except sqlalchemy.exc.SQLAlchemyError as e:
                raise PersistenceError(e) from e
            finally:
                if self.lock is not None:
                    # print(get_ident(), "releasing lock")
                    self.lock.release()
                if not self.is_scoped_session:
                    self.session.close()
                    assert self.token is not None
                    transactions.reset(self.token)


class SQLAlchemyDatastore:
    base_snapshot_record_cls = SnapshotRecord
    base_stored_event_record_cls = StoredEventRecord
    base_notification_tracking_record_cls = NotificationTrackingRecord
    record_classes: Dict[str, Tuple[Type[EventRecord], Type[EventRecord]]] = {}

    def __init__(
        self,
        *,
        session: Optional[scoped_session] = None,
        session_maker: Optional[sessionmaker] = None,
        url: Optional[str] = None,
        autoflush: bool = True,
        **engine_kwargs: Any,
    ):
        self.scoped_session: Optional[scoped_session] = None
        self.is_sqlite_in_memory_db = False
        self.is_sqlite_filedb = False
        self.is_sqlite_wal_mode = False
        self.access_lock: Optional[Semaphore] = None
        self.write_lock: Optional[Semaphore] = None
        self._tried_init_sqlite_wal_mode = False
        self._wal_mode_lock = Lock()

        if session is not None:
            self.set_scoped_session(session)
        elif session_maker is not None:
            self.session_maker: Optional[sessionmaker] = session_maker
            self.engine: Optional[Union[Engine, Connection]] = (
                self.session_maker().get_bind()
            )
        elif url:
            if url.startswith("sqlite"):
                if ":memory:" in url or "mode=memory" in url:
                    engine_kwargs = dict(engine_kwargs)
                    self.is_sqlite_in_memory_db = True
                    connect_args = engine_kwargs.get("connect_args") or {}
                    if "check_same_thread" not in connect_args:
                        connect_args["check_same_thread"] = False
                    engine_kwargs["connect_args"] = connect_args
                    if "poolclass" not in engine_kwargs:
                        engine_kwargs["poolclass"] = StaticPool
                    self.access_lock = Semaphore()
                else:
                    self.write_lock = Semaphore()

            engine = create_engine(url, echo=False, **engine_kwargs)
            self.is_sqlite_filedb = (
                engine.dialect.name == "sqlite" and not self.is_sqlite_in_memory_db
            )
            self.engine = engine
            self.session_maker = sessionmaker(bind=self.engine, autoflush=autoflush)

        else:
            self.engine = None
            self.session_maker = None

    def set_scoped_session(self, session: scoped_session) -> None:
        # assert isinstance(session, scoped_session)
        self.scoped_session = session
        self.access_lock = None
        self.write_lock = None
        self.engine = self.scoped_session.get_bind()
        self.session_maker = None  # self.scoped_session.session_factory

    def init_sqlite_wal_mode(self) -> None:
        self._tried_init_sqlite_wal_mode = True
        if self.is_sqlite_filedb and not self.is_sqlite_wal_mode:
            with self._wal_mode_lock:
                assert self.engine is not None
                with self.engine.connect() as connection:
                    cursor_result = connection.execute(text("PRAGMA journal_mode=WAL;"))
                    if list(cursor_result)[0][0] == "wal":
                        self.is_sqlite_wal_mode = True

    def transaction(self, commit: bool) -> Transaction:
        try:
            transaction = transactions.get()
            if commit is True and transaction.commit is False:
                raise ProgrammingError("Transaction already started with commit=False")
        except LookupError:
            if not self._tried_init_sqlite_wal_mode:
                # Do this after creating tables otherwise get disk I/0 error with SQLA v2.
                self.init_sqlite_wal_mode()
            lock: Optional[Semaphore] = None
            if self.access_lock:
                self.access_lock.acquire()
                lock = self.access_lock
            elif commit and self.write_lock:
                # print(get_ident(), "getting lock")
                self.write_lock.acquire()
                # print(get_ident(), "got lock")
                lock = self.write_lock
            if self.scoped_session is None:
                assert self.session_maker is not None
                session = self.session_maker()
            else:
                session = self.scoped_session
            transaction = Transaction(
                session,
                commit=commit,
                lock=lock,
                is_scoped_session=self.scoped_session is not None,
            )
            if self.scoped_session is None:
                transaction.token = transactions.set(transaction)
        return transaction

    @classmethod
    def define_record_class(
        cls,
        cls_name: str,
        table_name: str,
        schema_name: str | None,
        base_cls: Type[TEventRecord],
    ) -> Type[TEventRecord]:
        record_classes_key = (schema_name or "public") + "." + table_name
        try:
            record_class, record_base_cls = cls.record_classes[record_classes_key]
            if record_base_cls is not base_cls:
                raise ValueError(
                    f"Have already defined a record class with table name {table_name} "
                    f"from a different base class {record_base_cls}"
                )
        except KeyError:
            table_args: List[Any] = []
            for table_arg in base_cls.__dict__.get("__table_args__", []):
                if isinstance(table_arg, Index):
                    new_index = Index(
                        f"{table_name}_aggregate_idx",
                        unique=table_arg.unique,
                        *table_arg.expressions,  # noqa B026
                    )
                    table_args.append(new_index)
                else:
                    table_args.append(table_arg)
            if schema_name is not None:
                if table_args and isinstance(table_args[-1], dict):
                    table_args[-1]["schema"] = schema_name
                else:
                    table_args.append({"schema": schema_name})
            record_class = type(
                cls_name,
                (base_cls,),
                {
                    "__tablename__": table_name,
                    "__table_args__": tuple(table_args),
                },
            )
            cls.record_classes[record_classes_key] = (record_class, base_cls)
        return cast(Type[TEventRecord], record_class)

    @contextmanager
    def get_connection(self) -> Iterator[Connection]:
        try:
            assert self.engine
            conn = self.engine.connect()
            yield conn
        except Exception as e:
            try:
                raise exception_classes[type(e).__name__](str(e)) from e
            except KeyError:
                raise
