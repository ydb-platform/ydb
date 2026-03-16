import pytest
from quart import Quart
from quart.typing import TestClientProtocol

from .app import create_app

TestClientProtocol.__test__ = False  # type: ignore


@pytest.fixture
def app() -> Quart:
    # import app factory pattern
    app = create_app()

    # pushes an application context manually
    # ctx = app.app_context()
    # await ctx.push()
    return app


@pytest.fixture
def client(app: Quart) -> TestClientProtocol:
    return app.test_client()
