import asyncio
import asyncio.events
import contextlib
import errno
import pathlib
import socket

DEFAULT_BACKLOG = 50


class MultipleSocketServer(asyncio.events.AbstractServer):
    def __init__(self, servers):
        self._servers = servers

    def get_loop(self):
        return self._servers[0].get_loop()

    def start_serving(self):
        for server in self._servers:
            server.start_serving()

    async def serve_forever(self):
        raise NotImplementedError

    def is_serving(self):
        raise NotImplementedError

    @property
    def sockets(self):
        sockets = []
        for server in self._servers:
            sockets.extend(server.sockets)
        return sockets

    def close(self):
        for server in self._servers:
            server.close()

    async def wait_closed(self):
        for server in self._servers:
            server.close()


def create_tcp_server(
    factory,
    *,
    loop=None,
    host='localhost',
    port=0,
    sock=None,
    **kwargs,
):
    if sock is None:
        sock = bind_socket(host, port)
    return _create_server(factory, loop=loop, sock=sock, **kwargs)


def create_unix_server(
    factory,
    path: pathlib.Path,
    *,
    loop=None,
    sock=None,
    **kwargs,
):
    return _create_unix_server(
        factory, loop=loop, path=path, sock=sock, **kwargs
    )


@contextlib.asynccontextmanager
async def create_server_multiple(factory, sockets, *, loop=None, **kwargs):
    assert sockets
    if loop is None:
        loop = asyncio.get_running_loop()
    servers = []
    for sock in sockets:
        server = await loop.create_server(factory, sock=sock, **kwargs)
        servers.append(server)

    multi = MultipleSocketServer(servers)
    try:
        yield multi
    finally:
        multi.close()


async def start_multiple_servers(
    client_connected_cb, sockets, *, loop=None, **kwargs
) -> MultipleSocketServer:
    assert sockets

    servers = []
    for sock in sockets:
        server = await asyncio.start_server(
            client_connected_cb, sock=sock, **kwargs
        )
        servers.append(server)
    return MultipleSocketServer(servers)


def bind_socket_multiple(
    hostname='localhost',
    port=0,
    family=socket.AF_UNSPEC,
    type=socket.SOCK_STREAM,
    backlog=DEFAULT_BACKLOG,
    retries=15,
):
    """
    Bind multiple sockets for both IPv4 and IPv6 addresses.

    If `port` is zero tries to bind the same port for all addresses,
    `retries` times.
    """

    def bind():
        return _bind_socket_multiple(
            hostname,
            port,
            family=family,
            type=type,
            backlog=backlog,
        )

    if retries and not port:
        for _ in range(retries):
            try:
                return bind()
            except socket.error as err:
                if err.errno == errno.EADDRINUSE:
                    continue
    return bind()


def bind_socket(
    hostname='localhost',
    port=0,
    family=socket.AF_INET,
    type=socket.SOCK_STREAM,
    proto=-1,
    backlog=DEFAULT_BACKLOG,
):
    sock = socket.socket(family, type, proto)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((hostname, port))
    sock.listen(backlog)
    return sock


def bind_unix_socket(
    socket_path,
    backlog=DEFAULT_BACKLOG,
):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(str(socket_path))
    sock.listen(backlog)
    return sock


@contextlib.contextmanager
def closing_sockets(sockets):
    try:
        yield sockets
    finally:
        for sock in sockets:
            sock.close()


@contextlib.contextmanager
def _close_sockets_on_error(sockets):
    try:
        yield sockets
    except:
        for sock in sockets:
            sock.close()
        raise


@contextlib.asynccontextmanager
async def _create_server(factory, *, loop=None, **kwargs):
    if loop is None:
        loop = asyncio.get_running_loop()
    server = await loop.create_server(factory, **kwargs)
    try:
        yield server
    finally:
        server.close()


@contextlib.asynccontextmanager
async def _create_unix_server(factory, *, loop=None, **kwargs):
    if loop is None:
        loop = asyncio.get_running_loop()
    server = await loop.create_unix_server(factory, **kwargs)
    try:
        yield server
    finally:
        server.close()


def _bind_socket_multiple(
    hostname,
    port,
    *,
    family,
    type,
    backlog,
):
    """
    Bind multiple sockets for both IPv4 and IPv6 addresses.
    """
    infos = socket.getaddrinfo(
        hostname, port, family=family, type=type, flags=socket.AI_PASSIVE
    )
    with _close_sockets_on_error([]) as sockets:
        for af, socktype, proto, canonname, sa in infos:
            addr = sa[0]
            sock = bind_socket(
                addr, port, family=af, type=socktype, proto=proto
            )
            sock_port = sock.getsockname()[1]
            if port == 0:
                port = sock_port
            assert port == sock_port
            sockets.append(sock)
    return sockets
