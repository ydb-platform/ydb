import asyncio
import logging
from contextlib import asynccontextmanager

import httpx
import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from sse_starlette import EventSourceResponse

_log = logging.getLogger(__name__)
log_fmt = r"%(asctime)-15s %(levelname)s %(name)s %(funcName)s:%(lineno)d %(message)s"
datefmt = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(format=log_fmt, level=logging.DEBUG, datefmt=datefmt)

logging.getLogger("httpx").setLevel(logging.INFO)
logging.getLogger("httpcore").setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.INFO)
logging.getLogger("docker").setLevel(logging.INFO)


@pytest.fixture(autouse=True)
def reset_shutdown_state():
    """Reset shutdown state before/after each test.

    It ensures clean state for tests involving AppStatus and _shutdown_watcher.
    """
    from sse_starlette.sse import AppStatus, _thread_state

    # Setup: clean state
    AppStatus.should_exit = False
    AppStatus.enable_automatic_graceful_drain = True
    if hasattr(_thread_state, "shutdown_state"):
        del _thread_state.shutdown_state

    yield

    # Teardown: signal shutdown to kill any running watchers, then clean up
    AppStatus.should_exit = True
    AppStatus.enable_automatic_graceful_drain = True
    if hasattr(_thread_state, "shutdown_state"):
        del _thread_state.shutdown_state


@pytest.fixture
def anyio_backend():
    """Exclude trio from tests"""
    return "asyncio"


@pytest.fixture
async def app():
    @asynccontextmanager
    async def lifespan(app):
        # Startup
        _log.debug("Starting up")
        yield
        # Shutdown
        _log.debug("Shutting down")

    async def home():
        return PlainTextResponse("Hello, world!")

    async def endless(req: Request):
        async def event_publisher():
            i = 0
            try:
                while True:  # i <= 20:
                    # yield dict(id=..., event=..., data=...)
                    i += 1
                    print(f"Sending {i}")
                    yield dict(data=i)
                    await asyncio.sleep(0.3)
            except asyncio.CancelledError as e:
                _log.info(f"Disconnected from client (via refresh/close) {req.client}")
                # Do any other cleanup, if any
                raise e

        return EventSourceResponse(event_publisher())

    app = Starlette(
        routes=[Route("/", home), Route("/endless", endpoint=endless)],
        lifespan=lifespan,
    )

    async with LifespanManager(app):
        _log.info("We're in!")
        yield app
        _log.info("We're out!")


@pytest.fixture
async def httpx_client(app):
    transport = ASGITransport(app=app)

    async with httpx.AsyncClient(
        transport=transport, base_url="http://localhost:8000"
    ) as client:
        _log.info("Yielding Client")
        yield client


@pytest.fixture
def client(app):
    with TestClient(app=app, base_url="http://localhost:8000") as client:
        print("Yielding Client")
        yield client
