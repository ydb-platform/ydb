from __future__ import annotations

import socket
import struct
import warnings
from types import TracebackType
from typing import TYPE_CHECKING, Awaitable, Callable, NamedTuple, Optional, Tuple, Type

import aiohttp
import attrs
from yarl import URL

from .exceptions import DockerError


if TYPE_CHECKING:
    from .docker import Docker


class Message(NamedTuple):
    stream: int
    data: bytes


class Stream:
    _resp: aiohttp.ClientResponse | None

    def __init__(
        self,
        docker: "Docker",
        setup: Callable[[], Awaitable[Tuple[URL, Optional[bytes], bool]]],
        timeout: Optional[aiohttp.ClientTimeout] = None,
    ) -> None:
        self._setup = setup
        self.docker = docker
        self._resp = None
        self._closed = False
        self._timeout = timeout
        self._queue: Optional[aiohttp.FlowControlDataQueue[Message]] = None

    async def _init(self) -> None:
        if self._resp is not None:
            return
        url, body, tty = await self._setup()
        # inherit and update the parent client's timeout
        timeout = self.docker._timeout
        if self._timeout is not None:
            timeout = attrs.evolve(
                timeout,
                connect=self._timeout.connect,
                sock_connect=self._timeout.sock_connect,
            )
        # sock_read and total timeout doesn't make sense for streaming
        timeout = attrs.evolve(timeout, sock_read=None, total=None)
        self._resp = resp = await self.docker._do_query(
            url,
            method="POST",
            data=body,
            params=None,
            headers={"Connection": "Upgrade", "Upgrade": "tcp"},
            timeout=timeout,
            chunked=None,
            read_until_eof=False,
            versioned_api=True,
        )
        # read body if present, it can contain an information
        # about disconnection
        assert self._resp is not None
        body = await self._resp.read()

        conn = resp.connection
        if conn is None:
            msg = (
                "Cannot upgrade connection to vendored tcp protocol, "
                "the docker server has closed underlying socket."
            )
            msg += f" Status code: {resp.status}."
            msg += f" Headers: {resp.headers}."
            if body:
                if len(body) > 100:
                    msg = msg + f" First 100 bytes of body: [{body[100]!r}]..."
                else:
                    msg = msg + f" Body: [{body!r}]"
            raise DockerError(500, msg)
        protocol = conn.protocol
        loop = resp._loop
        assert protocol is not None
        assert protocol.transport is not None
        sock = protocol.transport.get_extra_info("socket")
        if sock is not None:
            # set TCP keepalive for vendored socket
            # the socket can be closed in the case of error
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        queue: aiohttp.FlowControlDataQueue[Message] = aiohttp.FlowControlDataQueue(
            protocol, limit=2**16, loop=loop
        )
        protocol.set_parser(_ExecParser(queue, tty=tty), queue)
        protocol.force_close()
        self._queue = queue

    async def read_out(self) -> Optional[Message]:
        """Read from stdout or stderr."""
        await self._init()
        try:
            assert self._queue is not None
            return await self._queue.read()
        except aiohttp.EofStream:
            return None

    async def write_in(self, data: bytes) -> None:
        """Write into stdin."""
        if self._closed:
            raise RuntimeError("Cannot write to closed transport")
        await self._init()
        assert self._resp is not None
        assert self._resp.connection is not None
        transport = self._resp.connection.transport
        assert transport is not None
        transport.write(data)
        protocol = self._resp.connection.protocol
        assert protocol is not None
        if protocol.transport is not None:
            await protocol._drain_helper()

    async def close(self) -> None:
        if self._resp is None:
            return
        if self._closed:
            return
        self._closed = True
        assert self._resp.connection is not None
        transport = self._resp.connection.transport
        if transport and transport.can_write_eof():
            transport.write_eof()
        self._resp.close()

    async def __aenter__(self) -> Stream:
        await self._init()
        return self

    async def __aexit__(
        self,
        exc_typ: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> Optional[bool]:
        await self.close()
        return None

    def __del__(self, _warnings=warnings) -> None:
        if self._resp is not None:
            return
        if not self._closed:
            warnings.warn("Unclosed ExecStream", ResourceWarning)


class _ExecParser:
    def __init__(self, queue, tty=False) -> None:
        self.queue = queue
        self.tty = tty
        self.header_fmt = struct.Struct(">BxxxL")
        self._buf = bytearray()

    def set_exception(self, exc: BaseException) -> None:
        self.queue.set_exception(exc)

    def feed_eof(self) -> None:
        self.queue.feed_eof()

    def feed_data(self, data: bytes) -> Tuple[bool, bytes]:
        if self.tty:
            msg = Message(1, data)  # stdout
            self.queue.feed_data(msg, len(data))
        else:
            self._buf.extend(data)
            while self._buf:
                # Parse the header
                if len(self._buf) < self.header_fmt.size:
                    return False, b""
                fileno, msglen = self.header_fmt.unpack(
                    self._buf[: self.header_fmt.size]
                )
                msg_and_header = self.header_fmt.size + msglen
                if len(self._buf) < msg_and_header:
                    return False, b""
                msg = Message(
                    fileno, bytes(self._buf[self.header_fmt.size : msg_and_header])
                )
                self.queue.feed_data(msg, msglen)
                del self._buf[:msg_and_header]
        return False, b""
