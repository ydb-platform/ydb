import pytest

from .app import Client


@pytest.fixture
def settings():
    return {}


@pytest.fixture
def client(settings):
    return Client(settings=settings)
