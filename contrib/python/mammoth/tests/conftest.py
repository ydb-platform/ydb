import funk
import pytest


@pytest.fixture(name="mocks")
def _fixture_mocks():
    mocks = funk.Mocks()
    yield mocks
    mocks.verify()
