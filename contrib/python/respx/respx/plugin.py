from typing import cast

import pytest

import respx

from .router import MockRouter


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "respx(assert_all_called=False, assert_all_mocked=False, base_url=...): "
        "configure the respx_mock fixture. "
        "See https://lundberg.github.io/respx/api.html#configuration",
    )


@pytest.fixture
def respx_mock(request):
    respx_marker = request.node.get_closest_marker("respx")

    mock_router: MockRouter = (
        respx.mock
        if respx_marker is None
        else cast(MockRouter, respx.mock(**respx_marker.kwargs))
    )

    with mock_router:
        yield mock_router
