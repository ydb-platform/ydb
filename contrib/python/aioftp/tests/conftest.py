import asyncio
import collections
import contextlib
import functools
import math
import socket
import ssl
import tempfile
import time
from pathlib import Path

import pytest
import pytest_asyncio
# import trustme
from async_timeout import timeout
# from siosocks.io.asyncio import socks_server_handler

import aioftp

# No ssl tests since https://bugs.python.org/issue36098
# ca = trustme.CA()
# server_cert = ca.issue_server_cert("127.0.0.1", "::1")
#
# ssl_server = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
# server_cert.configure_cert(ssl_server)
#
# ssl_client = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
# ca.configure_trust(ssl_client)


class Container:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


@pytest.fixture
def Client():
    return Container


@pytest.fixture
def Server():
    return Container


def _wrap_with_defaults(kwargs):
    test_defaults = dict(
        path_io_factory=aioftp.MemoryPathIO,
    )
    return collections.ChainMap(kwargs, test_defaults)


@pytest.fixture(params=["127.0.0.1", "::1"])
def pair_factory(request):
    class Factory:
        def __init__(
            self,
            client=None,
            server=None,
            *,
            connected=True,
            logged=True,
            do_quit=True,
            host=request.param,
            server_factory=aioftp.Server,
            client_factory=aioftp.Client,
        ):
            if client is None:
                client = Container()
            self.client = client_factory(*client.args, **_wrap_with_defaults(client.kwargs))
            if server is None:
                server = Container()
            self.server = server_factory(*server.args, **_wrap_with_defaults(server.kwargs))
            self.connected = connected
            self.logged = logged
            self.do_quit = do_quit
            self.host = host
            self.timeout = timeout(1)

        async def make_server_files(self, *paths, size=None, atom=b"-"):
            if size is None:
                size = aioftp.DEFAULT_BLOCK_SIZE * 3
            data = atom * size
            for p in paths:
                await self.client.make_directory(Path(p).parent)
                async with self.client.upload_stream(p) as stream:
                    await stream.write(data)

        async def make_client_files(self, *paths, size=None, atom=b"-"):
            if size is None:
                size = aioftp.DEFAULT_BLOCK_SIZE * 3
            data = atom * size
            for p in map(Path, paths):
                await self.client.path_io.mkdir(
                    p.parent,
                    parents=True,
                    exist_ok=True,
                )
                async with self.client.path_io.open(p, mode="wb") as f:
                    await f.write(data)

        async def server_paths_exists(self, *paths):
            values = []
            for p in paths:
                values.append(await self.client.exists(p))
            if all(values):
                return True
            if any(values):
                raise ValueError("Mixed exists/not exists list")
            return False

        async def client_paths_exists(self, *paths):
            values = []
            for p in paths:
                values.append(await self.client.path_io.exists(Path(p)))
            if all(values):
                return True
            if any(values):
                raise ValueError("Mixed exists/not exists list")
            return False

        async def __aenter__(self):
            await self.timeout.__aenter__()
            await self.server.start(host=self.host)
            if self.connected:
                await self.client.connect(
                    self.server.server_host,
                    self.server.server_port,
                )
                if self.logged:
                    await self.client.login()
            return self

        async def __aexit__(self, *exc_info):
            if self.connected and self.do_quit:
                await self.client.quit()
            self.client.close()
            await self.server.close()
            await self.timeout.__aexit__(*exc_info)

    return Factory


@pytest.fixture
def expect_codes_in_exception():
    @contextlib.contextmanager
    def context(*codes):
        try:
            yield
        except aioftp.StatusCodeError as e:
            assert set(e.received_codes) == set(codes)
        else:
            raise RuntimeError("There was no exception")

    return context


@pytest.fixture(
    params=[
        aioftp.MemoryPathIO,
        aioftp.PathIO,
        aioftp.AsyncPathIO,
    ],
)
def path_io(request):
    return request.param()


@pytest.fixture
def temp_dir(path_io):
    if isinstance(path_io, aioftp.MemoryPathIO):
        yield Path("/")
    else:
        with tempfile.TemporaryDirectory() as name:
            yield Path(name)


class Sleep:
    def __init__(self):
        self.delay = 0
        self.first_sleep = None

    async def sleep(self, delay, result=None, **kwargs):
        if self.first_sleep is None:
            self.first_sleep = time.monotonic()
        delay = (time.monotonic() - self.first_sleep) + delay
        self.delay = max(self.delay, delay)
        return result

    def is_close(self, delay, *, rel_tol=0.05, abs_tol=0.5):
        ok = math.isclose(self.delay, delay, rel_tol=rel_tol, abs_tol=abs_tol)
        if not ok:
            print(
                f"latest sleep: {self.delay}; expected delay: {delay}; rel: {rel_tol}",
            )
        return ok


@pytest.fixture
def skip_sleep(monkeypatch):
    with monkeypatch.context() as m:
        sleeper = Sleep()
        m.setattr(asyncio, "sleep", sleeper.sleep)
        yield sleeper


# @pytest_asyncio.fixture(
#     params=[
#         ("127.0.0.1", socket.AF_INET),
#         ("::1", socket.AF_INET6),
#     ],
# )
# async def socks(request, unused_tcp_port):
#     handler = functools.partial(
#         socks_server_handler,
#         allowed_versions={5},
#         username="foo",
#         password="bar",
#     )
#     Socks = collections.namedtuple("Socks", "host port server")
#     host, family = request.param
#     port = unused_tcp_port
#     server = await asyncio.start_server(
#         handler,
#         host=host,
#         port=port,
#         family=family,
#     )
#     yield Socks(host, port, server)
#     server.close()
#     await server.wait_closed()
