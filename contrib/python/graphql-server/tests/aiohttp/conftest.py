import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from .app import create_app


@pytest.fixture
def app():
    return create_app()


@pytest_asyncio.fixture
async def client(app):
    client = TestClient(TestServer(app))
    await client.start_server()
    yield client
    await client.close()
