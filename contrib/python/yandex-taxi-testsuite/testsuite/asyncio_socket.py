# mypy: disable-error-code=attr-defined


import asyncio
import select
import socket
import sys

DEFAULT_TIMEOUT = 10.0
_DefaultTimeout = object()

_ASYNCIO_HAS_SENDTO = sys.version_info >= (3, 11)


class AsyncioSocket:
    def __init__(
        self,
        sock: socket.socket,
        loop: asyncio.AbstractEventLoop | None = None,
        timeout=DEFAULT_TIMEOUT,
    ):
        if loop is None:
            loop = asyncio.get_running_loop()
        self._loop: asyncio.AbstractEventLoop = loop
        self._sock: socket.socket = sock
        self._default_timeout = timeout
        sock.setblocking(False)

    def __repr__(self):
        return f'<AsyncioSocket for {self._sock}>'

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @property
    def type(self):
        return self._sock.type

    @property
    def socket(self) -> socket.socket:
        return self._sock

    def fileno(self) -> int:
        return self._sock.fileno()

    async def connect(self, address, *, timeout=_DefaultTimeout):
        coro = self._loop.sock_connect(self._sock, address)
        return await self._with_timeout(coro, timeout=timeout)

    async def sendto(self, *args, timeout=_DefaultTimeout):
        # asyncio support added in version 3.11
        if not _ASYNCIO_HAS_SENDTO:
            return await self._sendto_legacy(*args, timeout=timeout)
        coro = self._loop.sock_sendto(self._sock, *args)
        try:
            return await self._with_timeout(coro, timeout=timeout)
        except NotImplementedError:
            return await self._sendto_legacy(*args, timeout=timeout)

    async def sendall(self, data, *, timeout=_DefaultTimeout):
        coro = self._loop.sock_sendall(self._sock, data)
        return await self._with_timeout(coro, timeout=timeout)

    async def recv(self, size, *, timeout=_DefaultTimeout):
        coro = self._loop.sock_recv(self._sock, size)
        return await self._with_timeout(coro, timeout=timeout)

    async def recvfrom(self, *args, timeout=_DefaultTimeout):
        # asyncio support added in version 3.11
        if not _ASYNCIO_HAS_SENDTO:
            return await self._recvfrom_legacy(*args, timeout=timeout)
        coro = self._loop.sock_recvfrom(self._sock, *args)
        try:
            return await self._with_timeout(coro, timeout=timeout)
        except NotImplementedError:
            return await self._recvfrom_legacy(*args, timeout=timeout)

    async def accept(self, *, timeout=_DefaultTimeout):
        coro = self._loop.sock_accept(self._sock)
        conn, address = await self._with_timeout(coro, timeout=timeout)
        return from_socket(conn), address

    def bind(self, address):
        return self._sock.bind(address)

    def listen(self, *args):
        return self._sock.listen(*args)

    def getsockname(self):
        return self._sock.getsockname()

    def setsockopt(self, *args, **kwargs):
        self._sock.setsockopt(*args, **kwargs)

    def close(self):
        self._sock.close()

    def has_data(self) -> bool:
        rlist, _, _ = select.select([self._sock], [], [], 0)
        return bool(rlist)

    def can_write(self) -> bool:
        _, wlist, _ = select.select([], [self._sock], [], 0)
        return bool(wlist)

    async def wait_for_data(self, timeout=_DefaultTimeout):
        if self.has_data():
            return
        coro = _wait_for_data(self._loop, self._sock)
        return await self._with_timeout(coro, timeout=timeout)

    async def _with_timeout(self, awaitable, timeout):
        # TODO(python3.11): switch to `asyncio.timeout()`
        if timeout is _DefaultTimeout:
            timeout = self._default_timeout

        return await asyncio.wait_for(awaitable, timeout=timeout)

    async def _sendto_legacy(self, *args, timeout):
        # uvloop and python < 3.11
        try:
            return self._sock.sendto(*args)
        except (BlockingIOError, InterruptedError):
            pass
        fut = self._loop.create_future()
        try:
            self._loop.add_writer(
                self.fileno(),
                _legacy_io_handler,
                fut,
                self._sock.sendto,
                *args,
            )
            return await self._with_timeout(fut, timeout=timeout)
        finally:
            self._loop.remove_writer(self.fileno())

    async def _recvfrom_legacy(self, *args, timeout):
        # uvloop and python < 3.11
        try:
            return self._sock.recvfrom(*args)
        except (BlockingIOError, InterruptedError):
            pass
        fut = self._loop.create_future()
        try:
            self._loop.add_reader(
                self.fileno(),
                _legacy_io_handler,
                fut,
                self._sock.recvfrom,
                *args,
            )
            return await self._with_timeout(fut, timeout=timeout)
        finally:
            self._loop.remove_reader(self.fileno())


class AsyncioSocketsFactory:
    def __init__(self, loop=None):
        if loop is None:
            loop = asyncio.get_running_loop()
        self._loop = loop

    def from_socket(self, sock, timeout=DEFAULT_TIMEOUT):
        return from_socket(sock, loop=self._loop, timeout=timeout)

    def socket(self, *args, timeout=DEFAULT_TIMEOUT):
        sock = socket.socket(*args)
        return self.from_socket(sock, timeout=timeout)

    async def getaddrinfo(self, *args, timeout=DEFAULT_TIMEOUT, **kwargs):
        coro = self._loop.getaddrinfo(*args, **kwargs)
        return await asyncio.wait_for(coro, timeout=timeout)

    def tcp(self, *, timeout=DEFAULT_TIMEOUT):
        return self.socket(socket.AF_INET, socket.SOCK_STREAM, timeout=timeout)

    def udp(self, *, timeout=DEFAULT_TIMEOUT):
        return self.socket(socket.AF_INET, socket.SOCK_DGRAM, timeout=timeout)

    def socketpair(self, *args, timeout=DEFAULT_TIMEOUT, **kwargs):
        sock1, sock2 = socket.socketpair(*args, **kwargs)
        return self.from_socket(sock1, timeout=timeout), self.from_socket(
            sock2, timeout=timeout
        )


def from_socket(
    sock: socket.socket | AsyncioSocket,
    *,
    loop=None,
    timeout=DEFAULT_TIMEOUT,
) -> AsyncioSocket:
    if isinstance(sock, AsyncioSocket):
        return sock
    return AsyncioSocket(sock, loop=loop, timeout=timeout)


def create_socket(*args, timeout=DEFAULT_TIMEOUT):
    return AsyncioSocketsFactory().socket(*args, timeout=timeout)


def create_tcp_socket(*args, timeout=DEFAULT_TIMEOUT):
    return AsyncioSocketsFactory().tcp(timeout=timeout)


def create_udp_socket(timeout=DEFAULT_TIMEOUT):
    return AsyncioSocketsFactory().udp(timeout=timeout)


def create_socketpair(*args, timeout=DEFAULT_TIMEOUT, **kwargs):
    return AsyncioSocketsFactory().socketpair(*args, **kwargs, timeout=timeout)


async def getaddrinfo(*args, **kwargs):
    return await AsyncioSocketsFactory().getaddrinfo(*args, **kwargs)


def _legacy_io_handler(fut, sock_handler, *args):
    if fut.done():
        return
    try:
        data = sock_handler(*args)
    except (BlockingIOError, InterruptedError):
        return  # try again next time
    except (SystemExit, KeyboardInterrupt):
        raise
    except BaseException as exc:
        fut.set_exception(exc)
    else:
        fut.set_result(data)


async def _wait_for_data(loop, sock):
    fut = loop.create_future()
    fd = sock.fileno()

    def on_data():
        if fut.done():
            return
        fut.set_result(None)

    try:
        loop.add_reader(fd, on_data)
        await fut
    finally:
        loop.remove_reader(fd)
