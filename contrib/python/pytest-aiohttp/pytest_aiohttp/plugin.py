import asyncio
import warnings
from typing import Any, Awaitable, Callable, Dict, Generator, Optional, Type, Union

import pytest
import pytest_asyncio
from aiohttp.test_utils import BaseTestServer, RawTestServer, TestClient, TestServer
from aiohttp.web import Application, BaseRequest, StreamResponse

AiohttpClient = Callable[[Union[Application, BaseTestServer]], Awaitable[TestClient]]


LEGACY_MODE = DeprecationWarning(
    "The 'asyncio_mode' is 'legacy', switching to 'auto' for the sake of "
    "pytest-aiohttp backward compatibility. "
    "Please explicitly use 'asyncio_mode=strict' or 'asyncio_mode=auto' "
    "in pytest configuration file."
)


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config) -> None:
    val = config.getoption("asyncio_mode")
    if val is None:
        val = config.getini("asyncio_mode")
    if val == "legacy":
        config.option.asyncio_mode = "auto"
        config.issue_config_time_warning(LEGACY_MODE, stacklevel=2)


@pytest.fixture
def loop(event_loop: asyncio.AbstractEventLoop) -> asyncio.AbstractEventLoop:
    warnings.warn(
        "'loop' fixture is deprecated and scheduled for removal, "
        "please use 'event_loop' instead",
        DeprecationWarning,
    )
    return event_loop


@pytest.fixture
def proactor_loop(event_loop: asyncio.AbstractEventLoop) -> asyncio.AbstractEventLoop:
    warnings.warn(
        "'proactor_loop' fixture is deprecated and scheduled for removal, "
        "please use 'event_loop' instead",
        DeprecationWarning,
    )
    return event_loop


@pytest.fixture
def aiohttp_unused_port(
    unused_tcp_port_factory: Callable[[], int]
) -> Callable[[], int]:
    warnings.warn(
        "'aiohttp_unused_port' fixture is deprecated "
        "and scheduled for removal, "
        "please use 'unused_tcp_port_factory' instead",
        DeprecationWarning,
    )
    return unused_tcp_port_factory


@pytest_asyncio.fixture
async def aiohttp_server() -> Callable[..., Awaitable[TestServer]]:
    """Factory to create a TestServer instance, given an app.

    aiohttp_server(app, **kwargs)
    """
    servers = []

    async def go(
        app: Application, *, port: Optional[int] = None, **kwargs: Any
    ) -> TestServer:
        server = TestServer(app, port=port)
        await server.start_server(**kwargs)
        servers.append(server)
        return server

    yield go

    while servers:
        await servers.pop().close()


@pytest_asyncio.fixture
async def aiohttp_raw_server() -> Callable[..., Awaitable[RawTestServer]]:
    """Factory to create a RawTestServer instance, given a web handler.

    aiohttp_raw_server(handler, **kwargs)
    """
    servers = []

    async def go(
        handler: Callable[[BaseRequest], Awaitable[StreamResponse]],
        *,
        port: Optional[int] = None,
        **kwargs: Any,
    ) -> RawTestServer:
        server = RawTestServer(handler, port=port)
        await server.start_server(**kwargs)
        servers.append(server)
        return server

    yield go

    while servers:
        await servers.pop().close()


@pytest.fixture
def aiohttp_client_cls() -> Type[TestClient]:
    """
    Client class to use in ``aiohttp_client`` factory.

    Use it for passing custom ``TestClient`` implementations.

    Example::

       class MyClient(TestClient):
           async def login(self, *, user, pw):
               payload = {"username": user, "password": pw}
               return await self.post("/login", json=payload)

       @pytest.fixture
       def aiohttp_client_cls():
           return MyClient

       def test_login(aiohttp_client):
           app = web.Application()
           client = await aiohttp_client(app)
           await client.login(user="admin", pw="s3cr3t")

    """
    return TestClient


@pytest_asyncio.fixture
async def aiohttp_client(
    aiohttp_client_cls: Type[TestClient],
) -> Generator[AiohttpClient, None, None]:
    """Factory to create a TestClient instance.

    aiohttp_client(app, **kwargs)
    aiohttp_client(server, **kwargs)
    aiohttp_client(raw_server, **kwargs)
    """
    clients = []

    async def go(
        __param: Union[Application, BaseTestServer],
        *,
        server_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> TestClient:
        if isinstance(__param, Application):
            server_kwargs = server_kwargs or {}
            server = TestServer(__param, **server_kwargs)
            client = aiohttp_client_cls(server, **kwargs)
        elif isinstance(__param, BaseTestServer):
            client = aiohttp_client_cls(__param, **kwargs)
        else:
            raise ValueError("Unknown argument type: %r" % type(__param))

        await client.start_server()
        clients.append(client)
        return client

    yield go

    while clients:
        await clients.pop().close()
