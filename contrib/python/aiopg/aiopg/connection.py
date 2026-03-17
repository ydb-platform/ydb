import abc
import asyncio
import contextlib
import ctypes
import datetime
import enum
import errno
import platform
import select
import socket
import sys
import traceback
import uuid
import warnings
import weakref
from collections.abc import Mapping
from types import TracebackType
from typing import (
    Any,
    Callable,
    Generator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    cast,
)

import psycopg2
import psycopg2.extensions
import psycopg2.extras

from .log import logger
from .utils import (
    ClosableQueue,
    _ContextManager,
    create_completed_future,
    get_running_loop,
)

TIMEOUT = 60.0

# Windows specific error code, not in errno for some reason, and doesnt map
# to OSError.errno EBADF
WSAENOTSOCK = 10038

# Connection status from psycopg2 (psycopg2/psycopg/connection.h)
CONN_STATUS_CONNECTING = 20

# In socket.socket we should know type and family to shutdown socket by fd
# This function is used for shutdown libpq connection
# where family and type is unknown
libc = ctypes.CDLL(None)
socket_shutdown = libc.shutdown
socket_shutdown.restypes = ctypes.c_int
socket_shutdown.argtypes = ctypes.c_int, ctypes.c_int


def connect(
    dsn: Optional[str] = None,
    *,
    timeout: float = TIMEOUT,
    enable_json: bool = True,
    enable_hstore: bool = True,
    enable_uuid: bool = True,
    echo: bool = False,
    **kwargs: Any,
) -> _ContextManager["Connection"]:
    """A factory for connecting to PostgreSQL.

    The coroutine accepts all parameters that psycopg2.connect() does
    plus optional keyword-only `timeout` parameters.

    Returns instantiated Connection object.

    """
    connection = Connection(
        dsn,
        timeout,
        bool(echo),
        enable_hstore=enable_hstore,
        enable_uuid=enable_uuid,
        enable_json=enable_json,
        **kwargs,
    )
    return _ContextManager[Connection](connection, disconnect)  # type: ignore


async def disconnect(c: "Connection") -> None:
    await c.close()


def _is_bad_descriptor_error(os_error: OSError) -> bool:
    if platform.system() == "Windows":  # pragma: no cover
        winerror = int(getattr(os_error, "winerror", 0))
        return winerror == WSAENOTSOCK
    return os_error.errno == errno.EBADF


class IsolationCompiler(abc.ABC):
    __slots__ = ("_isolation_level", "_readonly", "_deferrable")

    def __init__(
        self, isolation_level: Optional[str], readonly: bool, deferrable: bool
    ):
        self._isolation_level = isolation_level
        self._readonly = readonly
        self._deferrable = deferrable

    @property
    def name(self) -> str:
        return self._isolation_level or "Unknown"

    def savepoint(self, unique_id: str) -> str:
        return f"SAVEPOINT {unique_id}"

    def release_savepoint(self, unique_id: str) -> str:
        return f"RELEASE SAVEPOINT {unique_id}"

    def rollback_savepoint(self, unique_id: str) -> str:
        return f"ROLLBACK TO SAVEPOINT {unique_id}"

    def commit(self) -> str:
        return "COMMIT"

    def rollback(self) -> str:
        return "ROLLBACK"

    def begin(self) -> str:
        query = "BEGIN"
        if self._isolation_level is not None:
            query += f" ISOLATION LEVEL {self._isolation_level.upper()}"

        if self._readonly:
            query += " READ ONLY"

        if self._deferrable:
            query += " DEFERRABLE"

        return query

    def __repr__(self) -> str:
        return self.name


class ReadCommittedCompiler(IsolationCompiler):
    __slots__ = ()

    def __init__(self, readonly: bool, deferrable: bool):
        super().__init__("Read committed", readonly, deferrable)


class RepeatableReadCompiler(IsolationCompiler):
    __slots__ = ()

    def __init__(self, readonly: bool, deferrable: bool):
        super().__init__("Repeatable read", readonly, deferrable)


class SerializableCompiler(IsolationCompiler):
    __slots__ = ()

    def __init__(self, readonly: bool, deferrable: bool):
        super().__init__("Serializable", readonly, deferrable)


class DefaultCompiler(IsolationCompiler):
    __slots__ = ()

    def __init__(self, readonly: bool, deferrable: bool):
        super().__init__(None, readonly, deferrable)

    @property
    def name(self) -> str:
        return "Default"


class IsolationLevel(enum.Enum):
    serializable = SerializableCompiler
    repeatable_read = RepeatableReadCompiler
    read_committed = ReadCommittedCompiler
    default = DefaultCompiler

    def __call__(self, readonly: bool, deferrable: bool) -> IsolationCompiler:
        return self.value(readonly, deferrable)  # type: ignore


async def _release_savepoint(t: "Transaction") -> None:
    await t.release_savepoint()


async def _rollback_savepoint(t: "Transaction") -> None:
    await t.rollback_savepoint()


class Transaction:
    __slots__ = ("_cursor", "_is_begin", "_isolation", "_unique_id")

    def __init__(
        self,
        cursor: "Cursor",
        isolation_level: Callable[[bool, bool], IsolationCompiler],
        readonly: bool = False,
        deferrable: bool = False,
    ):
        self._cursor = cursor
        self._is_begin = False
        self._unique_id: Optional[str] = None
        self._isolation = isolation_level(readonly, deferrable)

    @property
    def is_begin(self) -> bool:
        return self._is_begin

    async def begin(self) -> "Transaction":
        if self._is_begin:
            raise psycopg2.ProgrammingError(
                "You are trying to open a new transaction, use the save point"
            )
        self._is_begin = True
        await self._cursor.execute(self._isolation.begin())
        return self

    async def commit(self) -> None:
        self._check_commit_rollback()
        await self._cursor.execute(self._isolation.commit())
        self._is_begin = False

    async def rollback(self) -> None:
        self._check_commit_rollback()
        if not self._cursor.closed:
            await self._cursor.execute(self._isolation.rollback())
        self._is_begin = False

    async def rollback_savepoint(self) -> None:
        self._check_release_rollback()
        if not self._cursor.closed:
            await self._cursor.execute(
                self._isolation.rollback_savepoint(
                    self._unique_id  # type: ignore
                )
            )
        self._unique_id = None

    async def release_savepoint(self) -> None:
        self._check_release_rollback()
        await self._cursor.execute(
            self._isolation.release_savepoint(self._unique_id)  # type: ignore
        )
        self._unique_id = None

    async def savepoint(self) -> "Transaction":
        self._check_commit_rollback()
        if self._unique_id is not None:
            raise psycopg2.ProgrammingError("You do not shut down savepoint")

        self._unique_id = f"s{uuid.uuid1().hex}"
        await self._cursor.execute(self._isolation.savepoint(self._unique_id))

        return self

    def point(self) -> _ContextManager["Transaction"]:
        return _ContextManager[Transaction](
            self.savepoint(),
            _release_savepoint,
            _rollback_savepoint,
        )

    def _check_commit_rollback(self) -> None:
        if not self._is_begin:
            raise psycopg2.ProgrammingError(
                "You are trying to commit " "the transaction does not open"
            )

    def _check_release_rollback(self) -> None:
        self._check_commit_rollback()
        if self._unique_id is None:
            raise psycopg2.ProgrammingError("You do not start savepoint")

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} "
            f"transaction={self._isolation} id={id(self):#x}>"
        )

    def __del__(self) -> None:
        if self._is_begin:
            warnings.warn(
                f"You have not closed transaction {self!r}", ResourceWarning
            )

        if self._unique_id is not None:
            warnings.warn(
                f"You have not closed savepoint {self!r}", ResourceWarning
            )

    async def __aenter__(self) -> "Transaction":
        return await self.begin()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()


async def _commit_transaction(t: Transaction) -> None:
    await t.commit()


async def _rollback_transaction(t: Transaction) -> None:
    await t.rollback()


class Cursor:
    def __init__(
        self,
        conn: "Connection",
        impl: Any,
        timeout: float,
        echo: bool,
        isolation_level: Optional[IsolationLevel] = None,
    ):
        self._conn = conn
        self._impl = impl
        self._timeout = timeout
        self._echo = echo
        self._transaction = Transaction(
            self, isolation_level or IsolationLevel.default
        )

    @property
    def echo(self) -> bool:
        """Return echo mode status."""
        return self._echo

    @property
    def description(self) -> Optional[Sequence[Any]]:
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences is a collections.namedtuple containing
        information describing one result column:

        0.  name: the name of the column returned.
        1.  type_code: the PostgreSQL OID of the column.
        2.  display_size: the actual length of the column in bytes.
        3.  internal_size: the size in bytes of the column associated to
            this column on the server.
        4.  precision: total number of significant digits in columns of
            type NUMERIC. None for other types.
        5.  scale: count of decimal digits in the fractional part in
            columns of type NUMERIC. None for other types.
        6.  null_ok: always None as not easy to retrieve from the libpq.

        This attribute will be None for operations that do not
        return rows or if the cursor has not had an operation invoked
        via the execute() method yet.

        """
        return self._impl.description  # type: ignore

    def close(self) -> None:
        """Close the cursor now."""
        if not self.closed:
            self._impl.close()

    @property
    def closed(self) -> bool:
        """Read-only boolean attribute: specifies if the cursor is closed."""
        return self._impl.closed  # type: ignore

    @property
    def connection(self) -> "Connection":
        """Read-only attribute returning a reference to the `Connection`."""
        return self._conn

    @property
    def raw(self) -> Any:
        """Underlying psycopg cursor object, readonly"""
        return self._impl

    @property
    def name(self) -> str:
        # Not supported
        return self._impl.name  # type: ignore

    @property
    def scrollable(self) -> Optional[bool]:
        # Not supported
        return self._impl.scrollable  # type: ignore

    @scrollable.setter
    def scrollable(self, val: bool) -> None:
        # Not supported
        self._impl.scrollable = val

    @property
    def withhold(self) -> bool:
        # Not supported
        return self._impl.withhold  # type: ignore

    @withhold.setter
    def withhold(self, val: bool) -> None:
        # Not supported
        self._impl.withhold = val

    async def execute(
        self,
        operation: str,
        parameters: Any = None,
        *,
        timeout: Optional[float] = None,
    ) -> None:
        """Prepare and execute a database operation (query or command).

        Parameters may be provided as sequence or mapping and will be
        bound to variables in the operation.  Variables are specified
        either with positional %s or named %({name})s placeholders.

        """
        if timeout is None:
            timeout = self._timeout
        waiter = self._conn._create_waiter("cursor.execute")
        if self._echo:
            logger.info(operation)
            logger.info("%r", parameters)
        try:
            self._impl.execute(operation, parameters)
        except BaseException:
            self._conn._waiter = None
            raise
        try:
            await self._conn._poll(waiter, timeout)
        except asyncio.TimeoutError:
            self._impl.close()
            raise

    async def executemany(self, *args: Any, **kwargs: Any) -> None:
        # Not supported
        raise psycopg2.ProgrammingError(
            "executemany cannot be used in asynchronous mode"
        )

    async def callproc(
        self,
        procname: str,
        parameters: Any = None,
        *,
        timeout: Optional[float] = None,
    ) -> None:
        """Call a stored database procedure with the given name.

        The sequence of parameters must contain one entry for each
        argument that the procedure expects. The result of the call is
        returned as modified copy of the input sequence. Input
        parameters are left untouched, output and input/output
        parameters replaced with possibly new values.

        """
        if timeout is None:
            timeout = self._timeout
        waiter = self._conn._create_waiter("cursor.callproc")
        if self._echo:
            logger.info("CALL %s", procname)
            logger.info("%r", parameters)
        try:
            self._impl.callproc(procname, parameters)
        except BaseException:
            self._conn._waiter = None
            raise
        else:
            await self._conn._poll(waiter, timeout)

    def begin(self) -> _ContextManager[Transaction]:
        return _ContextManager[Transaction](
            self._transaction.begin(),
            _commit_transaction,
            _rollback_transaction,
        )

    def begin_nested(self) -> _ContextManager[Transaction]:
        if self._transaction.is_begin:
            return self._transaction.point()

        return _ContextManager[Transaction](
            self._transaction.begin(),
            _commit_transaction,
            _rollback_transaction,
        )

    def mogrify(self, operation: str, parameters: Any = None) -> bytes:
        """Return a query string after arguments binding.

        The byte string returned is exactly the one that would be sent to
        the database running the .execute() method or similar.

        """
        ret = self._impl.mogrify(operation, parameters)
        assert (
            not self._conn.isexecuting()
        ), "Don't support server side mogrify"
        return ret  # type: ignore

    async def setinputsizes(self, sizes: int) -> None:
        """This method is exposed in compliance with the DBAPI.

        It currently does nothing but it is safe to call it.

        """
        self._impl.setinputsizes(sizes)

    async def fetchone(self) -> Any:
        """Fetch the next row of a query result set.

        Returns a single tuple, or None when no more data is
        available.

        """
        ret = self._impl.fetchone()
        assert (
            not self._conn.isexecuting()
        ), "Don't support server side cursors yet"
        return ret

    async def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        """Fetch the next set of rows of a query result.

        Returns a list of tuples. An empty list is returned when no
        more rows are available.

        The number of rows to fetch per call is specified by the
        parameter.  If it is not given, the cursor's .arraysize
        determines the number of rows to be fetched. The method should
        try to fetch as many rows as indicated by the size
        parameter. If this is not possible due to the specified number
        of rows not being available, fewer rows may be returned.

        """
        if size is None:
            size = self._impl.arraysize
        ret = self._impl.fetchmany(size)
        assert (
            not self._conn.isexecuting()
        ), "Don't support server side cursors yet"
        return ret  # type: ignore

    async def fetchall(self) -> List[Any]:
        """Fetch all (remaining) rows of a query result.

        Returns them as a list of tuples.  An empty list is returned
        if there is no more record to fetch.

        """
        ret = self._impl.fetchall()
        assert (
            not self._conn.isexecuting()
        ), "Don't support server side cursors yet"
        return ret  # type: ignore

    async def scroll(self, value: int, mode: str = "relative") -> None:
        """Scroll to a new position according to mode.

        If mode is relative (default), value is taken as offset
        to the current position in the result set, if set to
        absolute, value states an absolute target position.

        """
        self._impl.scroll(value, mode)
        assert (
            not self._conn.isexecuting()
        ), "Don't support server side cursors yet"

    @property
    def arraysize(self) -> int:
        """How many rows will be returned by fetchmany() call.

        This read/write attribute specifies the number of rows to
        fetch at a time with fetchmany(). It defaults to
        1 meaning to fetch a single row at a time.

        """
        return self._impl.arraysize  # type: ignore

    @arraysize.setter
    def arraysize(self, val: int) -> None:
        """How many rows will be returned by fetchmany() call.

        This read/write attribute specifies the number of rows to
        fetch at a time with fetchmany(). It defaults to
        1 meaning to fetch a single row at a time.

        """
        self._impl.arraysize = val

    @property
    def itersize(self) -> int:
        # Not supported
        return self._impl.itersize  # type: ignore

    @itersize.setter
    def itersize(self, val: int) -> None:
        # Not supported
        self._impl.itersize = val

    @property
    def rowcount(self) -> int:
        """Returns the number of rows that has been produced of affected.

        This read-only attribute specifies the number of rows that the
        last :meth:`execute` produced (for Data Query Language
        statements like SELECT) or affected (for Data Manipulation
        Language statements like UPDATE or INSERT).

        The attribute is -1 in case no .execute() has been performed
        on the cursor or the row count of the last operation if it
        can't be determined by the interface.

        """
        return self._impl.rowcount  # type: ignore

    @property
    def rownumber(self) -> int:
        """Row index.

        This read-only attribute provides the current 0-based index of the
        cursor in the result set or ``None`` if the index cannot be
        determined."""

        return self._impl.rownumber  # type: ignore

    @property
    def lastrowid(self) -> int:
        """OID of the last inserted row.

        This read-only attribute provides the OID of the last row
        inserted by the cursor. If the table wasn't created with OID
        support or the last operation is not a single record insert,
        the attribute is set to None.

        """
        return self._impl.lastrowid  # type: ignore

    @property
    def query(self) -> Optional[str]:
        """The last executed query string.

        Read-only attribute containing the body of the last query sent
        to the backend (including bound arguments) as bytes
        string. None if no query has been executed yet.

        """
        return self._impl.query  # type: ignore

    @property
    def statusmessage(self) -> str:
        """the message returned by the last command."""
        return self._impl.statusmessage  # type: ignore

    @property
    def tzinfo_factory(self) -> datetime.tzinfo:
        """The time zone factory used to handle data types such as
        `TIMESTAMP WITH TIME ZONE`.
        """
        return self._impl.tzinfo_factory  # type: ignore

    @tzinfo_factory.setter
    def tzinfo_factory(self, val: datetime.tzinfo) -> None:
        """The time zone factory used to handle data types such as
        `TIMESTAMP WITH TIME ZONE`.
        """
        self._impl.tzinfo_factory = val

    async def nextset(self) -> None:
        # Not supported
        self._impl.nextset()  # raises psycopg2.NotSupportedError

    async def setoutputsize(
        self, size: int, column: Optional[int] = None
    ) -> None:
        # Does nothing
        self._impl.setoutputsize(size, column)

    async def copy_from(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "copy_from cannot be used in asynchronous mode"
        )

    async def copy_to(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "copy_to cannot be used in asynchronous mode"
        )

    async def copy_expert(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "copy_expert cannot be used in asynchronous mode"
        )

    @property
    def timeout(self) -> float:
        """Return default timeout for cursor operations."""
        return self._timeout

    def __aiter__(self) -> "Cursor":
        return self

    async def __anext__(self) -> Any:
        ret = await self.fetchone()
        if ret is not None:
            return ret
        raise StopAsyncIteration

    async def __aenter__(self) -> "Cursor":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f"<"
            f"{type(self).__module__}::{type(self).__name__} "
            f"name={self.name}, "
            f"closed={self.closed}"
            f">"
        )


async def _close_cursor(c: Cursor) -> None:
    c.close()


class Connection:
    """Low-level asynchronous interface for wrapped psycopg2 connection.

    The Connection instance encapsulates a database session.
    Provides support for creating asynchronous cursors.

    """

    _source_traceback = None

    def __init__(
        self,
        dsn: Optional[str],
        timeout: float,
        echo: bool = False,
        enable_json: bool = True,
        enable_hstore: bool = True,
        enable_uuid: bool = True,
        **kwargs: Any,
    ):
        self._enable_json = enable_json
        self._enable_hstore = enable_hstore
        self._enable_uuid = enable_uuid
        self._loop = get_running_loop()
        self._waiter: Optional[
            "asyncio.Future[None]"
        ] = self._loop.create_future()

        kwargs["async_"] = kwargs.pop("async", True)
        kwargs.pop("loop", None)  # backward compatibility
        self._conn = psycopg2.connect(dsn, **kwargs)

        self._dsn = self._conn.dsn
        self._dns_params = self._conn.get_dsn_parameters()
        self._conn_timeout = self._dns_params.get('connect_timeout')
        if self._conn_timeout:
            self._conn_timeout = float(self._conn_timeout)

        assert self._conn.isexecuting(), "Is conn an async at all???"
        self._fileno: Optional[int] = self._conn.fileno()
        self._timeout = timeout
        self._last_usage = self._loop.time()
        self._writing = False
        self._echo = echo
        self._notifies = asyncio.Queue()  # type: ignore
        self._notifies_proxy = ClosableQueue(self._notifies, self._loop)
        self._weakref = weakref.ref(self)
        self._loop.add_reader(
            self._fileno, self._ready, self._weakref  # type: ignore
        )
        self._conn_timeout_handler = None
        if self._conn_timeout:
            self._conn_timeout_handler = self._loop.call_later(
                self._conn_timeout, self._shutdown, self._weakref)

        if self._loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))

    @staticmethod
    def _shutdown(weak_self):
        # Make sure that we won't get stuck in a blocking read or write
        # inside poll, then give libpq a chance to try another host.
        # If there is an error, we'll get it from poll.
        self = weak_self()

        # dead already.
        if self is None:
            return

        if self._conn.status == CONN_STATUS_CONNECTING:
            socket_shutdown(ctypes.c_int(self._fileno),
                            ctypes.c_int(socket.SHUT_RDWR))
            self._ready(self._weakref)

    @staticmethod
    def _ready(weak_self: "weakref.ref[Any]") -> None:
        self = cast(Connection, weak_self())
        if self is None:
            return

        waiter = self._waiter

        try:
            state = self._conn.poll()
            while self._conn.notifies:
                notify = self._conn.notifies.pop(0)
                self._notifies.put_nowait(notify)
        except (psycopg2.Warning, psycopg2.Error) as exc:
            if self._fileno is not None:
                try:
                    select.select([self._fileno], [], [], 0)
                except OSError as os_exc:
                    if _is_bad_descriptor_error(os_exc):
                        with contextlib.suppress(OSError):
                            self._loop.remove_reader(self._fileno)
                            # forget a bad file descriptor, don't try to
                            # touch it
                            self._fileno = None

            try:
                if self._writing:
                    self._writing = False
                    if self._fileno is not None:
                        self._loop.remove_writer(self._fileno)
            except OSError as exc2:
                if exc2.errno != errno.EBADF:
                    # EBADF is ok for closed file descriptor
                    # chain exception otherwise
                    exc2.__cause__ = exc
                    exc = exc2
            self._notifies_proxy.close(exc)
            if waiter is not None and not waiter.done():
                waiter.set_exception(exc)
        else:
            if self._fileno is None:
                # connection closed
                if waiter is not None and not waiter.done():
                    waiter.set_exception(
                        psycopg2.OperationalError("Connection closed")
                    )

            if self._conn.status == CONN_STATUS_CONNECTING \
                    and state != psycopg2.extensions.POLL_ERROR:
                # libpq could close and open new connection to the next host
                old_fileno = self._fileno
                self._fileno = self._conn.fileno()

                if self._conn_timeout_handler:
                    self._conn_timeout_handler.cancel()
                    self._conn_timeout_handler = self._loop.call_later(
                        self._conn_timeout, self._shutdown, self._weakref)

                with contextlib.suppress(OSError):
                    # if we are using select selector
                    self._loop.remove_reader(old_fileno)
                    if self._writing:
                        self._loop.remove_writer(old_fileno)

                self._loop.add_reader(self._fileno, self._ready, weak_self)
                if self._writing:
                    self._loop.add_writer(self._fileno, self._ready, weak_self)

            if state == psycopg2.extensions.POLL_OK:
                if self._writing:
                    self._loop.remove_writer(self._fileno)  # type: ignore
                    self._writing = False
                if waiter is not None and not waiter.done():
                    waiter.set_result(None)
            elif state == psycopg2.extensions.POLL_READ:
                if self._writing:
                    self._loop.remove_writer(self._fileno)  # type: ignore
                    self._writing = False
            elif state == psycopg2.extensions.POLL_WRITE:
                if not self._writing:
                    self._loop.add_writer(
                        self._fileno, self._ready, weak_self  # type: ignore
                    )
                    self._writing = True
            elif state == psycopg2.extensions.POLL_ERROR:
                self._fatal_error(
                    "Fatal error on aiopg connection: "
                    "POLL_ERROR from underlying .poll() call"
                )
            else:
                self._fatal_error(
                    f"Fatal error on aiopg connection: "
                    f"unknown answer {state} from underlying "
                    f".poll() call"
                )

    def _fatal_error(self, message: str) -> None:
        # Should be called from exception handler only.
        self._loop.call_exception_handler(
            {
                "message": message,
                "connection": self,
            }
        )
        self.close()
        if self._waiter and not self._waiter.done():
            self._waiter.set_exception(psycopg2.OperationalError(message))

    def _create_waiter(self, func_name: str) -> "asyncio.Future[None]":
        if self._waiter is not None:
            raise RuntimeError(
                f"{func_name}() called while another coroutine "
                f"is already waiting for incoming data"
            )
        self._waiter = self._loop.create_future()
        return self._waiter

    async def _poll(
        self, waiter: "asyncio.Future[None]", timeout: float
    ) -> None:
        assert waiter is self._waiter, (waiter, self._waiter)
        self._ready(self._weakref)

        try:
            await asyncio.wait_for(self._waiter, timeout)
        except (asyncio.CancelledError, asyncio.TimeoutError) as exc:
            await asyncio.shield(self.close())
            raise exc
        except psycopg2.extensions.QueryCanceledError as exc:
            self._loop.call_exception_handler(
                {
                    "message": exc.pgerror,
                    "exception": exc,
                    "future": self._waiter,
                }
            )
            raise asyncio.CancelledError
        finally:
            self._waiter = None

    def isexecuting(self) -> bool:
        return self._conn.isexecuting()  # type: ignore

    def cursor(
        self,
        name: Optional[str] = None,
        cursor_factory: Any = None,
        scrollable: Optional[bool] = None,
        withhold: bool = False,
        timeout: Optional[float] = None,
        isolation_level: Optional[IsolationLevel] = None,
    ) -> _ContextManager[Cursor]:
        """A coroutine that returns a new cursor object using the connection.

        *cursor_factory* argument can be used to create non-standard
         cursors. The argument must be subclass of
         `psycopg2.extensions.cursor`.

        *name*, *scrollable* and *withhold* parameters are not supported by
        psycopg in asynchronous mode.

        """

        self._last_usage = self._loop.time()
        coro = self._cursor(
            name=name,
            cursor_factory=cursor_factory,
            scrollable=scrollable,
            withhold=withhold,
            timeout=timeout,
            isolation_level=isolation_level,
        )
        return _ContextManager[Cursor](coro, _close_cursor)

    async def _cursor(
        self,
        name: Optional[str] = None,
        cursor_factory: Any = None,
        scrollable: Optional[bool] = None,
        withhold: bool = False,
        timeout: Optional[float] = None,
        isolation_level: Optional[IsolationLevel] = None,
    ) -> Cursor:
        if timeout is None:
            timeout = self._timeout

        impl = await self._cursor_impl(
            name=name,
            cursor_factory=cursor_factory,
            scrollable=scrollable,
            withhold=withhold,
        )
        cursor = Cursor(self, impl, timeout, self._echo, isolation_level)
        return cursor

    async def _cursor_impl(
        self,
        name: Optional[str] = None,
        cursor_factory: Any = None,
        scrollable: Optional[bool] = None,
        withhold: bool = False,
    ) -> Any:
        if cursor_factory is None:
            impl = self._conn.cursor(
                name=name, scrollable=scrollable, withhold=withhold
            )
        else:
            impl = self._conn.cursor(
                name=name,
                cursor_factory=cursor_factory,
                scrollable=scrollable,
                withhold=withhold,
            )
        return impl

    def _close(self) -> None:
        """Remove the connection from the event_loop and close it."""
        # N.B. If connection contains uncommitted transaction the
        # transaction will be discarded
        if self._fileno is not None:
            self._loop.remove_reader(self._fileno)
            if self._writing:
                self._writing = False
                self._loop.remove_writer(self._fileno)

        self._conn.close()

        if not self._loop.is_closed():
            if self._waiter is not None and not self._waiter.done():
                self._waiter.set_exception(
                    psycopg2.OperationalError("Connection closed")
                )

            self._notifies_proxy.close(
                psycopg2.OperationalError("Connection closed")
            )

    def close(self) -> "asyncio.Future[None]":
        self._close()
        return create_completed_future(self._loop)

    @property
    def closed(self) -> bool:
        """Connection status.

        Read-only attribute reporting whether the database connection is
        open (False) or closed (True).

        """
        return self._conn.closed  # type: ignore

    @property
    def raw(self) -> Any:
        """Underlying psycopg connection object, readonly"""
        return self._conn

    async def commit(self) -> None:
        raise psycopg2.ProgrammingError(
            "commit cannot be used in asynchronous mode"
        )

    async def rollback(self) -> None:
        raise psycopg2.ProgrammingError(
            "rollback cannot be used in asynchronous mode"
        )

    # TPC

    async def xid(
        self, format_id: int, gtrid: str, bqual: str
    ) -> Tuple[int, str, str]:
        return self._conn.xid(format_id, gtrid, bqual)  # type: ignore

    async def tpc_begin(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "tpc_begin cannot be used in asynchronous mode"
        )

    async def tpc_prepare(self) -> None:
        raise psycopg2.ProgrammingError(
            "tpc_prepare cannot be used in asynchronous mode"
        )

    async def tpc_commit(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "tpc_commit cannot be used in asynchronous mode"
        )

    async def tpc_rollback(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "tpc_rollback cannot be used in asynchronous mode"
        )

    async def tpc_recover(self) -> None:
        raise psycopg2.ProgrammingError(
            "tpc_recover cannot be used in asynchronous mode"
        )

    async def cancel(self) -> None:
        raise psycopg2.ProgrammingError(
            "cancel cannot be used in asynchronous mode"
        )

    async def reset(self) -> None:
        raise psycopg2.ProgrammingError(
            "reset cannot be used in asynchronous mode"
        )

    @property
    def dsn(self) -> Optional[str]:
        """DSN connection string.

        Read-only attribute representing dsn connection string used
        for connectint to PostgreSQL server.

        """
        return self._dsn  # type: ignore

    async def set_session(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "set_session cannot be used in asynchronous mode"
        )

    @property
    def autocommit(self) -> bool:
        """Autocommit status"""
        return self._conn.autocommit  # type: ignore

    @autocommit.setter
    def autocommit(self, val: bool) -> None:
        """Autocommit status"""
        self._conn.autocommit = val

    @property
    def isolation_level(self) -> int:
        """Transaction isolation level.

        The only allowed value is ISOLATION_LEVEL_READ_COMMITTED.

        """
        return self._conn.isolation_level  # type: ignore

    async def set_isolation_level(self, val: int) -> None:
        """Transaction isolation level.

        The only allowed value is ISOLATION_LEVEL_READ_COMMITTED.

        """
        self._conn.set_isolation_level(val)

    @property
    def encoding(self) -> str:
        """Client encoding for SQL operations."""
        return self._conn.encoding  # type: ignore

    async def set_client_encoding(self, val: str) -> None:
        self._conn.set_client_encoding(val)

    @property
    def notices(self) -> List[str]:
        """A list of all db messages sent to the client during the session."""
        return self._conn.notices  # type: ignore

    @property
    def cursor_factory(self) -> Any:
        """The default cursor factory used by .cursor()."""
        return self._conn.cursor_factory

    async def get_backend_pid(self) -> int:
        """Returns the PID of the backend server process."""
        return self._conn.get_backend_pid()  # type: ignore

    async def get_parameter_status(self, parameter: str) -> Optional[str]:
        """Look up a current parameter setting of the server."""
        return self._conn.get_parameter_status(parameter)  # type: ignore

    async def get_transaction_status(self) -> int:
        """Return the current session transaction status as an integer."""
        return self._conn.get_transaction_status()  # type: ignore

    @property
    def protocol_version(self) -> int:
        """A read-only integer representing protocol being used."""
        return self._conn.protocol_version  # type: ignore

    @property
    def server_version(self) -> int:
        """A read-only integer representing the backend version."""
        return self._conn.server_version  # type: ignore

    @property
    def status(self) -> int:
        """A read-only integer representing the status of the connection."""
        return self._conn.status  # type: ignore

    async def lobject(self, *args: Any, **kwargs: Any) -> None:
        raise psycopg2.ProgrammingError(
            "lobject cannot be used in asynchronous mode"
        )

    @property
    def timeout(self) -> float:
        """Return default timeout for connection operations."""
        return self._timeout

    @property
    def last_usage(self) -> float:
        """Return time() when connection was used."""
        return self._last_usage

    @property
    def echo(self) -> bool:
        """Return echo mode status."""
        return self._echo

    def __repr__(self) -> str:
        return (
            f"<"
            f"{type(self).__module__}::{type(self).__name__} "
            f"isexecuting={self.isexecuting()}, "
            f"closed={self.closed}, "
            f"echo={self.echo}, "
            f">"
        )

    def __del__(self) -> None:
        try:
            _conn = self._conn
        except AttributeError:
            return
        if _conn is not None and not _conn.closed:
            self.close()
            warnings.warn(f"Unclosed connection {self!r}", ResourceWarning)

            context = {"connection": self, "message": "Unclosed connection"}
            if self._source_traceback is not None:
                context["source_traceback"] = self._source_traceback
            self._loop.call_exception_handler(context)

    @property
    def notifies(self) -> ClosableQueue:
        """Return notification queue (an asyncio.Queue -like object)."""
        return self._notifies_proxy

    async def _get_oids(self) -> Tuple[Any, Any]:
        cursor = await self.cursor()
        rv0, rv1 = [], []
        try:
            await cursor.execute(
                "SELECT t.oid, typarray "
                "FROM pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid "
                "WHERE typname = 'hstore';"
            )

            async for oids in cursor:
                if isinstance(oids, Mapping):
                    rv0.append(oids["oid"])
                    rv1.append(oids["typarray"])
                else:
                    rv0.append(oids[0])
                    rv1.append(oids[1])
        finally:
            cursor.close()

        return tuple(rv0), tuple(rv1)

    async def _connect(self) -> "Connection":
        try:
            await self._poll(self._waiter, self._timeout)  # type: ignore
        except BaseException:
            await asyncio.shield(self.close())
            raise
        if self._enable_json:
            psycopg2.extras.register_default_json(self._conn)
        if self._enable_uuid:
            psycopg2.extras.register_uuid(conn_or_curs=self._conn)
        if self._enable_hstore:
            oid, array_oid = await self._get_oids()
            psycopg2.extras.register_hstore(
                self._conn, oid=oid, array_oid=array_oid
            )

        return self

    def __await__(self) -> Generator[Any, None, "Connection"]:
        return self._connect().__await__()

    async def __aenter__(self) -> "Connection":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        await self.close()
