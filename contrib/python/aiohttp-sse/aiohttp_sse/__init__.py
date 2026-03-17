import asyncio
import io
import re
import sys
from types import TracebackType
from typing import Any, Mapping, Optional, Type, TypeVar, Union, overload

from aiohttp.abc import AbstractStreamWriter
from aiohttp.web import BaseRequest, ContentCoding, Request, StreamResponse

from .helpers import _ContextManager

__version__ = "2.2.0"
__all__ = ["EventSourceResponse", "sse_response"]


class EventSourceResponse(StreamResponse):
    """This object could be used as regular aiohttp response for
    streaming data to client, usually browser with EventSource::

        async def hello(request):
            # create response object
            resp = await EventSourceResponse()
            async with resp:
                # stream data
                resp.send('foo')
            return resp
    """

    DEFAULT_PING_INTERVAL = 15
    DEFAULT_SEPARATOR = "\r\n"
    DEFAULT_LAST_EVENT_HEADER = "Last-Event-Id"
    LINE_SEP_EXPR = re.compile(r"\r\n|\r|\n")

    def __init__(
        self,
        *,
        status: int = 200,
        reason: Optional[str] = None,
        headers: Optional[Mapping[str, str]] = None,
        sep: Optional[str] = None,
    ):
        super().__init__(status=status, reason=reason)

        if headers is not None:
            self.headers.extend(headers)

        # mandatory for servers-sent events headers
        self.headers["Content-Type"] = "text/event-stream"
        self.headers["Cache-Control"] = "no-cache"
        self.headers["Connection"] = "keep-alive"
        self.headers["X-Accel-Buffering"] = "no"

        self._ping_interval: float = self.DEFAULT_PING_INTERVAL
        self._ping_task: Optional[asyncio.Task[None]] = None
        self._sep = sep if sep is not None else self.DEFAULT_SEPARATOR

    def is_connected(self) -> bool:
        """Check connection is prepared and ping task is not done."""
        if not self.prepared or self._ping_task is None:
            return False

        return not self._ping_task.done()

    async def _prepare(self, request: Request) -> "EventSourceResponse":
        # TODO(PY311): Use Self for return type.
        await self.prepare(request)
        return self

    async def prepare(self, request: BaseRequest) -> Optional[AbstractStreamWriter]:
        """Prepare for streaming and send HTTP headers.

        :param request: regular aiohttp.web.Request.
        """
        if not self.prepared:
            writer = await super().prepare(request)
            self._ping_task = asyncio.create_task(self._ping())
            # explicitly enabling chunked encoding, since content length
            # usually not known beforehand.
            self.enable_chunked_encoding()
            return writer
        else:
            # hackish way to check if connection alive
            # should be updated once we have proper API in aiohttp
            # https://github.com/aio-libs/aiohttp/issues/3105
            if request.protocol.transport is None:
                # request disconnected
                raise asyncio.CancelledError()
            return self._payload_writer

    async def send(
        self,
        data: str,
        id: Optional[str] = None,
        event: Optional[str] = None,
        retry: Optional[int] = None,
    ) -> None:
        """Send data using EventSource protocol

        :param str data: The data field for the message.
        :param str id: The event ID to set the EventSource object's last
            event ID value to.
        :param str event: The event's type. If this is specified, an event will
            be dispatched on the browser to the listener for the specified
            event name; the web site would use addEventListener() to listen
            for named events. The default event type is "message".
        :param int retry: The reconnection time to use when attempting to send
            the event. [What code handles this?] This must be an integer,
            specifying the reconnection time in milliseconds. If a non-integer
            value is specified, the field is ignored.
        """
        buffer = io.StringIO()
        if id is not None:
            buffer.write(self.LINE_SEP_EXPR.sub("", f"id: {id}"))
            buffer.write(self._sep)

        if event is not None:
            buffer.write(self.LINE_SEP_EXPR.sub("", f"event: {event}"))
            buffer.write(self._sep)

        for chunk in self.LINE_SEP_EXPR.split(data):
            buffer.write(f"data: {chunk}")
            buffer.write(self._sep)

        if retry is not None:
            if not isinstance(retry, int):
                raise TypeError("retry argument must be int")
            buffer.write(f"retry: {retry}")
            buffer.write(self._sep)

        buffer.write(self._sep)
        try:
            await self.write(buffer.getvalue().encode("utf-8"))
        except ConnectionResetError:
            self.stop_streaming()
            raise

    async def wait(self) -> None:
        """EventSourceResponse object is used for streaming data to the client,
        this method returns future, so we can wait until connection will
        be closed or other task explicitly call ``stop_streaming`` method.
        """
        if self._ping_task is None:
            raise RuntimeError("Response is not started")

        try:
            await self._ping_task
        except asyncio.CancelledError:
            if (
                sys.version_info >= (3, 11)
                and (task := asyncio.current_task())
                and task.cancelling()
            ):
                raise

    def stop_streaming(self) -> None:
        """Used in conjunction with ``wait`` could be called from other task
        to notify client that server no longer wants to stream anything.
        """
        if self._ping_task is None:
            raise RuntimeError("Response is not started")
        self._ping_task.cancel()

    def enable_compression(
        self, force: Union[bool, ContentCoding, None] = False
    ) -> None:
        raise NotImplementedError

    @property
    def last_event_id(self) -> Optional[str]:
        """Last event ID, requested by client."""
        if self._req is None:
            msg = "EventSource request must be prepared first"
            raise RuntimeError(msg)

        return self._req.headers.get(self.DEFAULT_LAST_EVENT_HEADER)

    @property
    def ping_interval(self) -> float:
        """Time interval between two ping massages"""
        return self._ping_interval

    @ping_interval.setter
    def ping_interval(self, value: float) -> None:
        """Setter for ping_interval property.

        :param value: interval in sec between two ping values.
        """

        if not isinstance(value, (int, float)):
            raise TypeError("ping interval must be int or float")
        if value < 0:
            raise ValueError("ping interval must be greater then 0")

        self._ping_interval = value

    async def _ping(self) -> None:
        # periodically send ping to the browser. Any message that
        # starts with ":" colon ignored by a browser and could be used
        # as ping message.
        message = ": ping{0}{0}".format(self._sep).encode("utf-8")
        while True:
            await asyncio.sleep(self._ping_interval)
            try:
                await self.write(message)
            except (ConnectionResetError, RuntimeError):
                # RuntimeError - on writing after EOF
                break

    async def __aenter__(self) -> "EventSourceResponse":
        # TODO(PY311): Use Self
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.stop_streaming()
        await self.wait()


# TODO(PY313): Use default and remove overloads.
ESR = TypeVar("ESR", bound=EventSourceResponse)


@overload
def sse_response(
    request: Request,
    *,
    status: int = 200,
    reason: Optional[str] = None,
    headers: Optional[Mapping[str, str]] = None,
    sep: Optional[str] = None,
) -> _ContextManager[EventSourceResponse]: ...


@overload
def sse_response(
    request: Request,
    *,
    status: int = 200,
    reason: Optional[str] = None,
    headers: Optional[Mapping[str, str]] = None,
    sep: Optional[str] = None,
    response_cls: Type[ESR],
) -> _ContextManager[ESR]: ...


def sse_response(
    request: Request,
    *,
    status: int = 200,
    reason: Optional[str] = None,
    headers: Optional[Mapping[str, str]] = None,
    sep: Optional[str] = None,
    response_cls: Type[EventSourceResponse] = EventSourceResponse,
) -> Any:
    if not issubclass(response_cls, EventSourceResponse):
        raise TypeError(
            "response_cls must be subclass of "
            "aiohttp_sse.EventSourceResponse, got {}".format(response_cls)
        )

    sse = response_cls(status=status, reason=reason, headers=headers, sep=sep)
    return _ContextManager(sse._prepare(request))
