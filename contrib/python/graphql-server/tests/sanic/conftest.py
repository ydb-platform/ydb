import pytest

from .app import create_app


@pytest.fixture
def app():
    return create_app()
