"""
An ``asyncio.Protocol`` subclass for lower level IO handling.
"""

import asyncio
import collections
import re
import ssl
from typing import Callable, cast

from .errors import (
    SMTPDataError,
    SMTPReadTimeoutError,
    SMTPResponseException,
    SMTPServerDisconnected,
    SMTPTimeoutError,
)
from .response import SMTPResponse
from .typing import SMTPStatus


__all__ = ("SMTPProtocol",)


MAX_LINE_LENGTH = 8192
LINE_ENDINGS_REGEX = re.compile(rb"(?:\r\n|\n|\r(?!\n))")
PERIOD_REGEX = re.compile(rb"(?m)^\.")


class FlowControlMixin(asyncio.Protocol):
    """
    Reusable flow control logic for StreamWriter.drain().
    This implements the protocol methods pause_writing(),
    resume_writing() and connection_lost().  If the subclass overrides
    these it must call the super methods.
    StreamWriter.drain() must wait for _drain_helper() coroutine.

    Copied from stdlib as per recommendation: https://bugs.python.org/msg343685.
    Logging and asserts removed, type annotations added.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        if loop is None:
            self._loop = asyncio.get_event_loop()
        else:
            self._loop = loop

        self._paused = False
        self._drain_waiters: collections.deque[asyncio.Future[None]] = (
            collections.deque()
        )
        self._connection_lost = False

    def pause_writing(self) -> None:
        self._paused = True

    def resume_writing(self) -> None:
        self._paused = False

        for waiter in self._drain_waiters:
            if not waiter.done():
                waiter.set_result(None)

    def connection_lost(self, exc: Exception | None) -> None:
        self._connection_lost = True
        # Wake up the writer(s) if currently paused.
        if not self._paused:
            return

        for waiter in self._drain_waiters:
            if not waiter.done():
                if exc is None:
                    waiter.set_result(None)
                else:
                    waiter.set_exception(exc)

    async def _drain_helper(self) -> None:
        if self._connection_lost:
            raise ConnectionResetError("Connection lost")
        if not self._paused:
            return
        waiter = self._loop.create_future()
        self._drain_waiters.append(waiter)
        try:
            await waiter
        finally:
            self._drain_waiters.remove(waiter)

    def _get_close_waiter(self, stream: asyncio.StreamWriter) -> "asyncio.Future[None]":
        raise NotImplementedError


class SMTPProtocol(FlowControlMixin, asyncio.BaseProtocol):
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop | None = None,
        connection_lost_callback: Callable[[], None] | None = None,
    ) -> None:
        super().__init__(loop=loop)
        self._over_ssl = False
        self._buffer = bytearray()
        self._response_waiter: asyncio.Future[SMTPResponse] | None = None

        self.transport: asyncio.BaseTransport | None = None
        self._command_lock: asyncio.Lock | None = None
        self._closed_future: "asyncio.Future[None]" = self._loop.create_future()
        self._quit_sent = False
        self._connection_lost_callback = connection_lost_callback

    def _get_close_waiter(self, stream: asyncio.StreamWriter) -> "asyncio.Future[None]":
        return self._closed_future

    def __del__(self) -> None:
        # Avoid 'Future exception was never retrieved' warnings
        # Some unknown race conditions can sometimes trigger these :(
        self._retrieve_response_exception()

    @property
    def is_connected(self) -> bool:
        """
        Check if our transport is still connected.
        """
        return bool(self.transport is not None and not self.transport.is_closing())

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = cast(asyncio.Transport, transport)
        self._over_ssl = transport.get_extra_info("sslcontext") is not None
        self._response_waiter = self._loop.create_future()
        self._command_lock = asyncio.Lock()
        self._quit_sent = False

    def connection_lost(self, exc: Exception | None) -> None:
        super().connection_lost(exc)

        if not self._quit_sent:
            smtp_exc = SMTPServerDisconnected("Connection lost")
            if exc:
                smtp_exc.__cause__ = exc

            if self._response_waiter and not self._response_waiter.done():
                self._response_waiter.set_exception(smtp_exc)

        self.transport = None
        self._command_lock = None

        if self._connection_lost_callback:
            self._connection_lost_callback()

    def data_received(self, data: bytes) -> None:
        if self._response_waiter is None:
            raise RuntimeError(
                f"data_received called without a response waiter set: {data!r}"
            )
        elif self._response_waiter.done():
            # We got a response without issuing a command; ignore it.
            return

        self._buffer.extend(data)

        # If we got an obvious partial message, don't try to parse the buffer
        last_linebreak = data.rfind(b"\n")
        if (
            last_linebreak == -1
            or data[last_linebreak + 3 : last_linebreak + 4] == b"-"
        ):
            return

        try:
            response = self._read_response_from_buffer()
        except Exception as exc:
            self._response_waiter.set_exception(exc)
        else:
            if response is not None:
                self._response_waiter.set_result(response)

    def eof_received(self) -> bool:
        exc = SMTPServerDisconnected("Unexpected EOF received")
        if self._response_waiter and not self._response_waiter.done():
            self._response_waiter.set_exception(exc)

        # Returning false closes the transport
        return False

    def _retrieve_response_exception(self) -> BaseException | None:
        """
        Return any exception that has been set on the response waiter.

        Used to avoid 'Future exception was never retrieved' warnings
        """
        if (
            self._response_waiter
            and self._response_waiter.done()
            and not self._response_waiter.cancelled()
        ):
            return self._response_waiter.exception()

        return None

    def _read_response_from_buffer(self) -> SMTPResponse | None:
        """Parse the actual response (if any) from the data buffer"""
        code = -1
        message = bytearray()
        offset = 0
        message_complete = False

        while True:
            line_end_index = self._buffer.find(b"\n", offset)
            if line_end_index == -1:
                break

            line = bytes(self._buffer[offset : line_end_index + 1])

            if len(line) > MAX_LINE_LENGTH:
                raise SMTPResponseException(
                    SMTPStatus.unrecognized_command, "Response too long"
                )

            try:
                code = int(line[:3])
            except ValueError:
                error_text = line.decode("utf-8", errors="ignore")
                raise SMTPResponseException(
                    SMTPStatus.invalid_response.value,
                    f"Malformed SMTP response line: {error_text}",
                ) from None

            offset += len(line)
            if len(message):
                message.extend(b"\n")
            message.extend(line[4:].strip(b" \t\r\n"))
            if line[3:4] != b"-":
                message_complete = True
                break

        if message_complete:
            response = SMTPResponse(
                code, bytes(message).decode("utf-8", "surrogateescape")
            )
            del self._buffer[:offset]
            return response
        else:
            return None

    async def read_response(self, timeout: float | None = None) -> SMTPResponse:
        """
        Get a status response from the server.

        This method must be awaited once per command sent; if multiple commands
        are written to the transport without awaiting, response data will be lost.

        Returns an :class:`.response.SMTPResponse` namedtuple consisting of:
          - server response code (e.g. 250, or such, if all goes well)
          - server response string (multiline responses are converted to a
            single, multiline string).
        """
        if self._response_waiter is None:
            raise SMTPServerDisconnected("Connection lost")

        try:
            result = await asyncio.wait_for(self._response_waiter, timeout)
        except (TimeoutError, asyncio.TimeoutError) as exc:
            raise SMTPReadTimeoutError("Timed out waiting for server response") from exc
        finally:
            # If we were disconnected, don't create a new waiter
            if self.transport is None:
                self._response_waiter = None
            else:
                self._response_waiter = self._loop.create_future()

        return result

    def write(self, data: bytes) -> None:
        if self.transport is None or self.transport.is_closing():
            raise SMTPServerDisconnected("Connection lost")

        try:
            cast(asyncio.WriteTransport, self.transport).write(data)
        # uvloop raises NotImplementedError, asyncio doesn't have a write method
        except (AttributeError, NotImplementedError):
            raise RuntimeError(
                f"Transport {self.transport!r} does not support writing."
            ) from None

    async def execute_command(
        self, *args: bytes, timeout: float | None = None
    ) -> SMTPResponse:
        """
        Sends an SMTP command along with any args to the server, and returns
        a response.
        """
        if self._command_lock is None:
            raise SMTPServerDisconnected("Server not connected")
        command = b" ".join(args) + b"\r\n"

        async with self._command_lock:
            self.write(command)

            if command == b"QUIT\r\n":
                self._quit_sent = True

            response = await self.read_response(timeout=timeout)

        return response

    async def execute_data_command(
        self, message: bytes, timeout: float | None = None
    ) -> SMTPResponse:
        """
        Sends an SMTP DATA command to the server, followed by encoded message content.

        Automatically quotes lines beginning with a period per RFC821.
        Lone \\\\r and \\\\n characters are converted to \\\\r\\\\n
        characters.
        """
        if self._command_lock is None:
            raise SMTPServerDisconnected("Server not connected")

        message = LINE_ENDINGS_REGEX.sub(b"\r\n", message)
        message = PERIOD_REGEX.sub(b"..", message)
        if not message.endswith(b"\r\n"):
            message += b"\r\n"
        message += b".\r\n"

        async with self._command_lock:
            self.write(b"DATA\r\n")
            start_response = await self.read_response(timeout=timeout)
            if start_response.code != SMTPStatus.start_input:
                raise SMTPDataError(start_response.code, start_response.message)

            self.write(message)
            response = await self.read_response(timeout=timeout)
            if response.code != SMTPStatus.completed:
                raise SMTPDataError(response.code, response.message)

        return response

    async def start_tls(
        self,
        tls_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> SMTPResponse:
        """
        Puts the connection to the SMTP server into TLS mode.
        """
        if self._over_ssl:
            raise RuntimeError("Already using TLS.")
        if self._command_lock is None:
            raise SMTPServerDisconnected("Server not connected")

        async with self._command_lock:
            self.write(b"STARTTLS\r\n")
            response = await self.read_response(timeout=timeout)
            if response.code != SMTPStatus.ready:
                raise SMTPResponseException(response.code, response.message)

            # Check for disconnect after response
            if self.transport is None or self.transport.is_closing():
                raise SMTPServerDisconnected("Connection lost")

            try:
                tls_transport = await self._loop.start_tls(
                    cast(asyncio.WriteTransport, self.transport),
                    self,
                    tls_context,
                    server_side=False,
                    server_hostname=server_hostname,
                    ssl_handshake_timeout=timeout,
                )
            except (TimeoutError, asyncio.TimeoutError) as exc:
                raise SMTPTimeoutError("Timed out while upgrading transport") from exc
            # SSLProtocol only raises ConnectionAbortedError on timeout
            except ConnectionAbortedError as exc:
                raise SMTPTimeoutError(
                    "Connection aborted while upgrading transport"
                ) from exc
            except ConnectionError as exc:
                raise SMTPServerDisconnected(
                    "Connection reset while upgrading transport"
                ) from exc

            self.transport = tls_transport

        return response
