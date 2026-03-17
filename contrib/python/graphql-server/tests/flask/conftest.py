import pytest

from .app import create_app


@pytest.fixture
def app():
    # import app factory pattern
    app = create_app()

    # pushes an application context manually
    ctx = app.app_context()
    ctx.push()
    return app


@pytest.fixture
def client(app):
    return app.test_client()
