from unittest import mock

import pytest


@pytest.fixture
def mock_sleep():
    with mock.patch("time.sleep") as mocked:
        yield mocked
