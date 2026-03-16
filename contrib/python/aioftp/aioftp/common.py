import abc
import asyncio
import collections
import functools
import locale
import socket
import ssl
import sys
import threading
from collections.abc import AsyncIterator, Awaitable, Callable, Generator, Iterable, MutableMapping
from contextlib import contextmanager
from typing import Any, Generic, Protocol, TypeVar, overload

if sys.version_info >= (3, 11):
    from typing import ParamSpec, Self
else:
    from typing_extensions import ParamSpec, Self


__all__ = (
    "with_timeout",
    "StreamIO",
    "Throttle",
    "StreamThrottle",
    "ThrottleStreamIO",
    "END_OF_LINE",
    "DEFAULT_BLOCK_SIZE",
    "wrap_with_container",
    "AsyncStreamIterator",
    "AbstractAsyncLister",
    "AsyncListerMixin",
    "async_enterable",
    "DEFAULT_PORT",
    "DEFAULT_USER",
    "DEFAULT_PASSWORD",
    "DEFAULT_ACCOUNT",
    "setlocale",
    "SSLSessionBoundContext",
    "Code",
    "wrap_into_codes",
    "Connection",
)

END_OF_LINE = "\r\n"
DEFAULT_BLOCK_SIZE = 8192

DEFAULT_PORT = 21
DEFAULT_USER = "anonymous"
DEFAULT_PASSWORD = "anon@"
DEFAULT_ACCOUNT = ""
HALF_OF_YEAR_IN_SECONDS = 15778476
TWO_YEARS_IN_SECONDS = ((365 * 3 + 366) * 24 * 60 * 60) / 2


def _now() -> float:
    return asyncio.get_running_loop().time()


WithTimeOutParamSpec = ParamSpec("WithTimeOutParamSpec")
WithTimeOutReturnType = TypeVar("WithTimeOutReturnType")


def _with_timeout(
    name: str,
) -> Callable[
    [Callable[WithTimeOutParamSpec, Awaitable[WithTimeOutReturnType]]],
    Callable[WithTimeOutParamSpec, Awaitable[WithTimeOutReturnType]],
]:
    def decorator(
        f: Callable[WithTimeOutParamSpec, Awaitable[WithTimeOutReturnType]],
    ) -> Callable[WithTimeOutParamSpec, Awaitable[WithTimeOutReturnType]]:
        @functools.wraps(f)
        async def wrapper(
            *args: WithTimeOutParamSpec.args,
            **kwargs: WithTimeOutParamSpec.kwargs,
        ) -> WithTimeOutReturnType:
            cls = args[0]
            timeout = getattr(cls, name)
            return await asyncio.wait_for(f(*args, **kwargs), timeout)

        return wrapper

    return decorator


@overload
def with_timeout(
    name: str,
) -> Callable[
    [Callable[WithTimeOutParamSpec, Awaitable[WithTimeOutReturnType]]],
    Callable[WithTimeOutParamSpec, Awaitable[WithTimeOutReturnType]],
]: ...


@overload
def with_timeout(
    name: Callable[WithTimeOutParamSpec, Awaitable[WithTimeOutReturnType]],
) -> Callable[WithTimeOutParamSpec, Awaitable[WithTimeOutReturnType]]: ...


def with_timeout(
    name: str | Callable[WithTimeOutParamSpec, Awaitable[WithTimeOutReturnType]],
) -> (
    Callable[
        [Callable[WithTimeOutParamSpec, Awaitable[WithTimeOutReturnType]]],
        Callable[WithTimeOutParamSpec, Awaitable[WithTimeOutReturnType]],
    ]
    | Callable[WithTimeOutParamSpec, Awaitable[WithTimeOutReturnType]]
):
    """
    Method decorator, wraps method with :py:func:`asyncio.wait_for`. `timeout`
    argument takes from `name` decorator argument or "timeout".

    :param name: name of timeout attribute
    :type name: :py:class:`str`

    :raises asyncio.TimeoutError: if coroutine does not finished in timeout

    Wait for `self.timeout`
    ::

        >>> def __init__(self, ...):
        ...
        ...     self.timeout = 1
        ...
        ... @with_timeout
        ... async def foo(self, ...):
        ...
        ...     pass

    Wait for custom timeout
    ::

        >>> def __init__(self, ...):
        ...
        ...     self.foo_timeout = 1
        ...
        ... @with_timeout("foo_timeout")
        ... async def foo(self, ...):
        ...
        ...     pass

    """

    if isinstance(name, str):
        return _with_timeout(name)
    else:
        return _with_timeout("timeout")(name)


class AsyncStreamIterator:
    def __init__(self, read_coro: Callable[[], Awaitable[bytes]]) -> None:
        self.read_coro = read_coro

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> bytes:
        data = await self.read_coro()
        if data:
            return data
        else:
            raise StopAsyncIteration


IterableType = TypeVar("IterableType")


class AsyncListerMixin(abc.ABC, Generic[IterableType]):
    """
    Add ability to `async for` context to collect data to list via await.

    ::

        >>> class Context(AsyncListerMixin):
        ...     ...
        >>> results = await Context(...)
    """

    async def _to_list(self) -> list[IterableType]:
        items = []
        async for item in self:
            items.append(item)
        return items

    def __await__(self) -> Generator[None, None, list[IterableType]]:
        return self._to_list().__await__()

    @abc.abstractmethod
    def __aiter__(self) -> AsyncIterator[IterableType]:
        """
        Must be implemented by subclasses to enable iteration using ``async for``.
        :rtype: :py:class:`typing.AsyncIterator`[:py:class:`IterableType`]
        """


class AbstractAsyncLister(AsyncListerMixin[IterableType], abc.ABC):
    """
    Abstract context with ability to collect all iterables into
    :py:class:`list` via `await` with optional timeout (via
    :py:func:`aioftp.with_timeout`)

    :param timeout: timeout for __anext__ operation
    :type timeout: :py:class:`None`, :py:class:`int` or :py:class:`float`

    ::

        >>> class Lister(AbstractAsyncLister):
        ...
        ...     @with_timeout
        ...     async def __anext__(self):
        ...         ...

    ::

        >>> async for block in Lister(...):
        ...     ...

    ::

        >>> result = await Lister(...)
        >>> result
        [block, block, block, ...]
    """

    def __init__(self, *, timeout: float | int | None = None) -> None:
        super().__init__()
        self.timeout = timeout

    def __aiter__(self) -> Self:
        return self

    @with_timeout
    @abc.abstractmethod
    async def __anext__(self) -> IterableType:
        """
        :py:func:`asyncio.coroutine`

        Abstract method
        """


class AsyncContextManager(Protocol):
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *args: Any, **kwargs: Any) -> None: ...


AsyncEnterableReturnType = TypeVar("AsyncEnterableReturnType", bound=AsyncContextManager, covariant=True)


class AsyncEnterableInstanceProtocol(Protocol[AsyncEnterableReturnType]):
    async def __aenter__(self) -> AsyncEnterableReturnType: ...
    async def __aexit__(self, *args: Any, **kwargs: Any) -> None: ...
    def __await__(self) -> Generator[None, None, AsyncEnterableReturnType]: ...


AsyncEnterableParamSpec = ParamSpec("AsyncEnterableParamSpec")


def async_enterable(
    f: Callable[AsyncEnterableParamSpec, Awaitable[AsyncEnterableReturnType]],
) -> Callable[AsyncEnterableParamSpec, AsyncEnterableInstanceProtocol[AsyncEnterableReturnType]]:
    """
    Decorator. Bring coroutine result up, so it can be used as async context

    ::

        >>> async def foo():
        ...
        ...     ...
        ...     return AsyncContextInstance(...)
        ...
        ... ctx = await foo()
        ... async with ctx:
        ...
        ...     # do

    ::

        >>> @async_enterable
        ... async def foo():
        ...
        ...     ...
        ...     return AsyncContextInstance(...)
        ...
        ... async with foo() as ctx:
        ...
        ...     # do
        ...
        ... ctx = await foo()
        ... async with ctx:
        ...
        ...     # do

    """

    @functools.wraps(f)
    def wrapper(
        *args: AsyncEnterableParamSpec.args,
        **kwargs: AsyncEnterableParamSpec.kwargs,
    ) -> AsyncEnterableInstanceProtocol[AsyncEnterableReturnType]:
        class AsyncEnterableInstance:
            async def __aenter__(self) -> AsyncEnterableReturnType:
                self.context = await f(*args, **kwargs)
                return await self.context.__aenter__()

            async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
                await self.context.__aexit__(*args, **kwargs)

            def __await__(self) -> Generator[None, None, AsyncEnterableReturnType]:
                return f(*args, **kwargs).__await__()

        return AsyncEnterableInstance()

    return wrapper


class Code(str):
    """
    Representation of server status code.
    """

    def matches(self, mask: str) -> bool:
        """
        :param mask: Template for comparision. If mask symbol is not digit
            then it passes.
        :type mask: :py:class:`str`

        ::

            >>> Code("123").matches("1")
            True
            >>> Code("123").matches("1x3")
            True
        """
        return all(map(lambda m, c: not m.isdigit() or m == c, mask, self))


StrType = TypeVar("StrType", bound=str)
NotStrType = TypeVar("NotStrType")  # typing doesn't support ~T to exclude all subclasses of str


@overload
def wrap_with_container(o: StrType) -> tuple[StrType]: ...
@overload
def wrap_with_container(o: NotStrType) -> NotStrType: ...


def wrap_with_container(o: StrType | NotStrType) -> tuple[StrType] | NotStrType:
    if isinstance(o, str):
        return (o,)  # type: ignore[return-value]
    return o


def wrap_into_codes(o: Iterable[str]) -> tuple[Code, ...]:
    return tuple(Code(item) for item in o if not isinstance(item, Code))


class StreamIO:
    """
    Stream input/output wrapper with timeout.

    :param reader: stream reader
    :type reader: :py:class:`asyncio.StreamReader`

    :param writer: stream writer
    :type writer: :py:class:`asyncio.StreamWriter`

    :param timeout: socket timeout for read/write operations
    :type timeout: :py:class:`int`, :py:class:`float` or :py:class:`None`

    :param read_timeout: socket timeout for read operations, overrides
        `timeout`
    :type read_timeout: :py:class:`int`, :py:class:`float` or :py:class:`None`

    :param write_timeout: socket timeout for write operations, overrides
        `timeout`
    :type write_timeout: :py:class:`int`, :py:class:`float` or :py:class:`None`
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        timeout: float | int | None = None,
        read_timeout: float | int | None = None,
        write_timeout: float | int | None = None,
    ):
        self.reader = reader
        self.writer = writer
        self.read_timeout = read_timeout or timeout
        self.write_timeout = write_timeout or timeout

    @with_timeout("read_timeout")
    async def readline(self) -> bytes:
        """
        :py:func:`asyncio.coroutine`

        Proxy for :py:meth:`asyncio.StreamReader.readline`.
        """
        return await self.reader.readline()

    @with_timeout("read_timeout")
    async def read(self, count: int = -1) -> bytes:
        """
        :py:func:`asyncio.coroutine`

        Proxy for :py:meth:`asyncio.StreamReader.read`.

        :param count: block size for read operation
        :type count: :py:class:`int`
        """
        return await self.reader.read(count)

    @with_timeout("read_timeout")
    async def readexactly(self, count: int) -> bytes:
        """
        :py:func:`asyncio.coroutine`

        Proxy for :py:meth:`asyncio.StreamReader.readexactly`.

        :param count: block size for read operation
        :type count: :py:class:`int`
        """
        return await self.reader.readexactly(count)

    @with_timeout("write_timeout")
    async def write(self, data: bytes) -> None:
        """
        :py:func:`asyncio.coroutine`

        Combination of :py:meth:`asyncio.StreamWriter.write` and
        :py:meth:`asyncio.StreamWriter.drain`.

        :param data: data to write
        :type data: :py:class:`bytes`
        """
        self.writer.write(data)
        await self.writer.drain()

    def close(self) -> None:
        """
        Close connection.
        """
        self.writer.close()

    async def start_tls(self, sslcontext: ssl.SSLContext, server_hostname: str | None) -> None:
        """
        Upgrades the connection to TLS
        """
        await self.writer.start_tls(  # type: ignore[attr-defined,unused-ignore]  # py3.12+
            sslcontext=sslcontext,
            server_hostname=server_hostname,
            ssl_handshake_timeout=self.write_timeout,
        )


class Throttle:
    """
    Throttle for streams.

    :param limit: speed limit in bytes or :py:class:`None` for unlimited
    :type limit: :py:class:`int` or :py:class:`None`

    :param reset_rate: time in seconds for «round» throttle memory (to deal
        with float precision when divide)
    :type reset_rate: :py:class:`int` or :py:class:`float`
    """

    def __init__(self, *, limit: int | None = None, reset_rate: float | int = 10) -> None:
        self._limit = limit
        self.reset_rate = reset_rate
        self._start: float | None = None
        self._sum = 0

    async def wait(self) -> None:
        """
        :py:func:`asyncio.coroutine`

        Wait until can do IO
        """
        if self._limit is not None and self._limit > 0 and self._start is not None:
            now = _now()
            end = self._start + self._sum / self._limit
            await asyncio.sleep(max(0, end - now))

    def append(self, data: bytes, start: float) -> None:
        """
        Count `data` for throttle

        :param data: bytes of data for count
        :type data: :py:class:`bytes`

        :param start: start of read/write time from
            :py:meth:`asyncio.BaseEventLoop.time`
        :type start: :py:class:`float`
        """
        if self._limit is not None and self._limit > 0:
            if self._start is None:
                self._start = start
            if start - self._start > self.reset_rate:
                self._sum -= round((start - self._start) * self._limit)
                self._start = start
            self._sum += len(data)

    @property
    def limit(self) -> int | None:
        """
        Throttle limit
        """
        return self._limit

    @limit.setter
    def limit(self, value: int | None) -> None:
        """
        Set throttle limit

        :param value: bytes per second
        :type value: :py:class:`int` or :py:class:`None`
        """
        self._limit = value
        self._start = None
        self._sum = 0

    def clone(self) -> "Throttle":
        """
        Clone throttle without memory
        """
        return Throttle(limit=self._limit, reset_rate=self.reset_rate)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(limit={self._limit!r}, reset_rate={self.reset_rate!r})"


class StreamThrottle(collections.namedtuple("StreamThrottle", "read write")):
    """
    Stream throttle with `read` and `write` :py:class:`aioftp.Throttle`

    :param read: stream read throttle
    :type read: :py:class:`aioftp.Throttle`

    :param write: stream write throttle
    :type write: :py:class:`aioftp.Throttle`
    """

    def clone(self) -> "StreamThrottle":
        """
        Clone throttles without memory
        """
        return StreamThrottle(
            read=self.read.clone(),
            write=self.write.clone(),
        )

    @classmethod
    def from_limits(
        cls,
        read_speed_limit: int | None = None,
        write_speed_limit: int | None = None,
    ) -> "StreamThrottle":
        """
        Simple wrapper for creation :py:class:`aioftp.StreamThrottle`

        :param read_speed_limit: stream read speed limit in bytes or
            :py:class:`None` for unlimited
        :type read_speed_limit: :py:class:`int` or :py:class:`None`

        :param write_speed_limit: stream write speed limit in bytes or
            :py:class:`None` for unlimited
        :type write_speed_limit: :py:class:`int` or :py:class:`None`
        """
        return cls(
            read=Throttle(limit=read_speed_limit),
            write=Throttle(limit=write_speed_limit),
        )


class ThrottleStreamIO(StreamIO):
    """
    Throttled :py:class:`aioftp.StreamIO`. `ThrottleStreamIO` is subclass of
    :py:class:`aioftp.StreamIO`. `throttles` attribute is dictionary of `name`:
    :py:class:`aioftp.StreamThrottle` pairs

    :param *args: positional arguments for :py:class:`aioftp.StreamIO`
    :param **kwargs: keyword arguments for :py:class:`aioftp.StreamIO`

    :param throttles: dictionary of throttles
    :type throttles: :py:class:`dict` with :py:class:`aioftp.Throttle` values

    ::

        >>> self.stream = ThrottleStreamIO(
        ...     reader,
        ...     writer,
        ...     throttles={
        ...         "main": StreamThrottle(
        ...             read=Throttle(...),
        ...             write=Throttle(...)
        ...         )
        ...     },
        ...     timeout=timeout
        ... )
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        throttles: dict[str, StreamThrottle] = {},
        *,
        timeout: float | int | None = None,
        read_timeout: float | int | None = None,
        write_timeout: float | int | None = None,
    ):
        super().__init__(reader, writer, timeout=timeout, read_timeout=read_timeout, write_timeout=write_timeout)
        self.throttles = throttles

    async def wait(self, name: str) -> None:
        """
        :py:func:`asyncio.coroutine`

        Wait for all throttles

        :param name: name of throttle to acquire ("read" or "write")
        :type name: :py:class:`str`
        """
        tasks = []
        for throttle in self.throttles.values():
            curr_throttle = getattr(throttle, name)
            if curr_throttle.limit:
                tasks.append(asyncio.create_task(curr_throttle.wait()))
        if tasks:
            await asyncio.wait(tasks)

    def append(self, name: str, data: bytes, start: float) -> None:
        """
        Update timeout for all throttles

        :param name: name of throttle to append to ("read" or "write")
        :type name: :py:class:`str`

        :param data: bytes of data for count
        :type data: :py:class:`bytes`

        :param start: start of read/write time from
            :py:meth:`asyncio.BaseEventLoop.time`
        :type start: :py:class:`float`
        """
        for throttle in self.throttles.values():
            getattr(throttle, name).append(data, start)

    async def read(self, count: int = -1) -> bytes:
        """
        :py:func:`asyncio.coroutine`

        :py:meth:`aioftp.StreamIO.read` proxy
        """
        await self.wait("read")
        start = _now()
        data = await super().read(count)
        self.append("read", data, start)
        return data

    async def readline(self) -> bytes:
        """
        :py:func:`asyncio.coroutine`

        :py:meth:`aioftp.StreamIO.readline` proxy
        """
        await self.wait("read")
        start = _now()
        data = await super().readline()
        self.append("read", data, start)
        return data

    async def write(self, data: bytes) -> None:
        """
        :py:func:`asyncio.coroutine`

        :py:meth:`aioftp.StreamIO.write` proxy
        """
        await self.wait("write")
        start = _now()
        await super().write(data)
        self.append("write", data, start)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: Any) -> None:
        self.close()

    def iter_by_line(self) -> AsyncStreamIterator:
        """
        Read/iterate stream by line.

        :rtype: :py:class:`aioftp.AsyncStreamIterator`

        ::

            >>> async for line in stream.iter_by_line():
            ...     ...
        """
        return AsyncStreamIterator(self.readline)

    def iter_by_block(self, count: int = DEFAULT_BLOCK_SIZE) -> AsyncStreamIterator:
        """
        Read/iterate stream by block.

        :rtype: :py:class:`aioftp.AsyncStreamIterator`

        ::

            >>> async for block in stream.iter_by_block(block_size):
            ...     ...
        """
        return AsyncStreamIterator(lambda: self.read(count))


LOCALE_LOCK = threading.Lock()


@contextmanager
def setlocale(name: str) -> Generator[str, None, None]:
    """
    Context manager with threading lock for set locale on enter, and set it
    back to original state on exit.

    ::

        >>> with setlocale("C"):
        ...     ...
    """
    with LOCALE_LOCK:
        old_locale = locale.setlocale(locale.LC_ALL)
        try:
            yield locale.setlocale(locale.LC_ALL, name)
        finally:
            locale.setlocale(locale.LC_ALL, old_locale)


# class from https://github.com/python/cpython/issues/79152 (with some changes)
class SSLSessionBoundContext(ssl.SSLContext):
    """ssl.SSLContext bound to an existing SSL session.

    Actually asyncio doesn't support TLS session resumption, the loop.create_connection() API
    does not take any TLS session related argument. There is ongoing work to add support for this
    at https://github.com/python/cpython/issues/79152.

    The loop.create_connection() API takes a SSL context argument though, the SSLSessionBoundContext
    is used to wrap a SSL context and inject a SSL session on calls to
        - SSLSessionBoundContext.wrap_socket()
        - SSLSessionBoundContext.wrap_bio()

    This wrapper is compatible with any TLS application which calls only the methods above when
    making new TLS connections. This class is NOT a subclass of ssl.SSLContext, so it will be
    rejected by applications which ensure the SSL context is an instance of ssl.SSLContext. Not being
    a subclass of ssl.SSLContext makes this wrapper lightweight.
    """

    __slots__ = ("context", "session")

    def __init__(self, protocol: int, context: ssl.SSLContext, session: ssl.SSLSession):
        self.context = context
        self.session = session

    def wrap_socket(
        self,
        sock: socket.socket,
        server_side: bool = False,
        do_handshake_on_connect: bool = True,
        suppress_ragged_eofs: bool = True,
        server_hostname: str | bytes | None = None,
        session: ssl.SSLSession | None = None,
    ) -> ssl.SSLSocket:
        if session is not None:
            raise ValueError("expected session to be None")
        return self.context.wrap_socket(
            sock=sock,
            server_hostname=server_hostname,
            server_side=server_side,
            do_handshake_on_connect=do_handshake_on_connect,
            suppress_ragged_eofs=suppress_ragged_eofs,
            session=self.session,
        )

    def wrap_bio(
        self,
        incoming: ssl.MemoryBIO,
        outgoing: ssl.MemoryBIO,
        server_side: bool = False,
        server_hostname: str | bytes | None = None,
        session: ssl.SSLSession | None = None,
    ) -> ssl.SSLObject:
        if session is not None:
            raise ValueError("expected session to be None")
        return self.context.wrap_bio(
            incoming=incoming,
            outgoing=outgoing,
            server_hostname=server_hostname,
            server_side=server_side,
            session=self.session,
        )


class Connection(collections.defaultdict[str, asyncio.Future[Any]]):
    """
    Connection state container for transparent work with futures for async
    wait

    :param kwargs: initialization parameters

    Container based on :py:class:`collections.defaultdict`, which holds
    :py:class:`asyncio.Future` as default factory. There is two layers of
    abstraction:

    * Low level based on simple dictionary keys to attributes mapping and
        available at Connection.future.
    * High level based on futures result and dictionary keys to attributes
        mapping and available at Connection.

    To clarify, here is groups of equal expressions
    ::

        >>> connection.future.foo
        >>> connection["foo"]

        >>> connection.foo
        >>> connection["foo"].result()

        >>> del connection.future.foo
        >>> del connection.foo
        >>> del connection["foo"]
    """

    __slots__ = ("future",)

    class Container:
        def __init__(self, storage: MutableMapping[str, asyncio.Future[Any]]) -> None:
            self.storage = storage

        def __getattr__(self, name: str) -> asyncio.Future[Any]:
            return self.storage[name]

        def __delattr__(self, name: str) -> None:
            self.storage.pop(name)

    future: "Connection.Container"
    default_factory: Callable[[], asyncio.Future[Any]]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(asyncio.Future)
        self.future = Connection.Container(self)
        for k, v in kwargs.items():
            self[k].set_result(v)

    def __getattr__(self, name: str) -> Any:
        if name in self:
            return self[name].result()
        raise AttributeError(f"{name!r} not in storage")

    def __setattr__(self, name: str, value: Any) -> None:
        if name in Connection.__slots__:
            super().__setattr__(name, value)
        else:
            if self[name].done():
                self[name] = self.default_factory()
            self[name].set_result(value)

    def __delattr__(self, name: str) -> None:
        if name in self:
            self.pop(name)
