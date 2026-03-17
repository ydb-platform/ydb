"""
Logcapture allows to intercepts service logs on demand with context manager.

It starts tcp server and read logs sent by server.
"""

import asyncio
import collections
import contextlib
import enum
import logging
import typing

from testsuite.utils import callinfo, net, traceback

logger = logging.getLogger(__name__)


class BaseError(Exception):
    pass


class IncorrectUsageError(BaseError):
    """Incorrect usage error."""


class ClientConnectTimeoutError(BaseError):
    pass


class TimeoutError(BaseError):
    pass


class LogLevel(enum.IntEnum):
    """
    Represents log level as IntEnum, which supports comparison.

    Available levels are: TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL, NONE
    """

    TRACE = 0
    DEBUG = 1
    INFO = 2
    WARNING = 3
    ERROR = 4
    CRITICAL = 5
    NONE = 6

    @classmethod
    def from_string(cls, level: str) -> 'LogLevel':
        """Parse log level from the string."""
        return cls[level.upper()]


class CapturedLogs:
    def __init__(self, *, log_level: LogLevel) -> None:
        self._log_level = log_level
        self._logs: list[dict] = []
        self._subscribers = []
        self._closed = False

    @property
    def log_level(self):
        return self._log_level

    def is_closed(self):
        return self._closed

    def close(self):
        self._closed = True

    async def publish(self, row: dict) -> None:
        self._logs.append(row)
        for query, callback in self._subscribers:
            if _match_entry(row, query):
                await callback(**row)

    def subscribe(self, query: dict, decorated):
        self._subscribers.append((query, decorated))

    def __iter__(self) -> typing.Iterator[dict]:
        return iter(self._logs)


class Capture:
    def __init__(self, logs: CapturedLogs):
        self._logs = logs

    def select(self, **query) -> list[dict]:
        """Select logs matching query.

        Could only be used after capture contextmanager block.

        .. code-block:: python

           async with logcapture_server.capture() as capture:
               ...
           records = capture.select(text='Message to capture')
        """
        if not self._logs.is_closed():
            raise IncorrectUsageError(
                'select() is only supported for closed captures\n'
                'Please move select() after context manager body',
            )
        level = query.get('level')
        if level:
            log_level = LogLevel[level]
            if log_level.value < self._logs.log_level.value:
                raise IncorrectUsageError(
                    f'Requested log level={log_level.name} is lower than service log level {self._logs.log_level.name}',
                )
        result = []
        for row in self._logs:
            if _match_entry(row, query):
                result.append(row)
        return result

    def subscribe(self, **query):
        """Subscribe to records matching `query`. Returns decorator function.
        `subscribe()` may only be used within `capture()` block. Callqueue is returned.

        .. code-block:: python

           async with logcapture_server.capture() as capture:
               @capture.subscribe(text='Message to capture')
               def log_event(link, **other):
                   ...
               ...
               assert log_event.wait_call()
        """
        if self._logs.is_closed():
            raise IncorrectUsageError(
                'subscribe() is not supported for closed captures\nPlease move subscribe() into context manager body',
            )

        def decorator(func):
            decorated = callinfo.acallqueue(func)
            self._logs.subscribe(query, decorated)
            return decorated

        return decorator


class CaptureServer:
    _capture: CapturedLogs | None

    def __init__(
        self,
        *,
        log_level: LogLevel,
        parse_line: collections.abc.Callable[[bytes], dict],
    ):
        """Capture server."""
        self._log_level = log_level
        self._client_cond = asyncio.Condition()
        self._capture = None
        self._tasks = []
        self._parse_line = parse_line
        self._started = False
        self._socknames = []

    @property
    def default_log_level(self) -> LogLevel:
        """Returns default log level specified on object creation."""
        return self._log_level

    def getsocknames(self) -> list[tuple]:
        """Return list of server socket names."""
        return self._socknames

    @contextlib.asynccontextmanager
    async def start(
        self, host='localhost', port=0, **kwargs
    ) -> typing.AsyncIterator['CaptureServer']:
        """Starts capture logs asyncio server.

        Arguments are directly passed to `asyncio.start_server`. Server could be started
        only once. Capture server is returned. Server is closed when contextmanager
        is finished.
        """
        if self._started:
            raise IncorrectUsageError('Service was already started')

        sockets = net.bind_socket_multiple(host, port)
        server = await net.start_multiple_servers(
            self._handle_client, sockets, **kwargs
        )
        self._started = True
        self._socknames = [sock.getsockname() for sock in server.sockets]
        logger.debug('Logcapture server bound to %r', self._socknames)
        try:
            yield self
        finally:
            server.close()
            await server.wait_closed()

    async def wait_for_client(self, timeout: float = 10.0):
        """Waits for logserver client to connect."""

        async def waiter():
            async with self._client_cond:
                await self._client_cond.wait_for(lambda: self._tasks)

        logger.debug('Waiting for logcapture client to connect...')
        try:
            await asyncio.wait_for(waiter(), timeout=timeout)
        except TimeoutError:
            raise ClientConnectTimeoutError(
                'Timedout while waiting for logcapture client to connect',
            )

    async def _handle_client(self, reader, writer):
        logger.debug('logcapture client connected')

        async def log_reader(capture: CapturedLogs):
            with contextlib.closing(writer):
                try:
                    async for line in reader:
                        row = self._parse_line(line)
                        await capture.publish(row)
                except Exception:
                    async for line in reader:
                        # wait for data transfer to finish
                        pass
                    raise
            await writer.wait_closed()

        if not self._capture:
            writer.close()
            await writer.wait_closed()
        else:
            self._tasks.append(asyncio.create_task(log_reader(self._capture)))
            async with self._client_cond:
                self._client_cond.notify_all()

    @contextlib.asynccontextmanager
    async def capture(
        self,
        *,
        log_level: LogLevel | None = None,
        timeout: float = 10.0,
    ) -> typing.AsyncIterator[Capture]:
        """
        Starts logs capture. Returns `Capture` object.
        """
        if self._capture:
            yield self._capture
            return

        self._capture = CapturedLogs(log_level=log_level or self._log_level)
        try:
            yield Capture(self._capture)
        finally:
            self._capture.close()
            self._capture = None
            if self._tasks:
                _, pending = await asyncio.wait(self._tasks, timeout=timeout)
                self._tasks = []
                if pending:
                    raise TimeoutError(
                        'Timeout while waiting for capture task to finish',
                    )


def _match_entry(row: dict, query: dict) -> bool:
    for key, value in query.items():
        if row.get(key) != value:
            return False
    return True


__tracebackhide__ = traceback.hide(BaseError, FileNotFoundError)
