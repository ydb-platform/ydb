from collections.abc import Generator
from operator import methodcaller

import httpx
import pytest
from pytest import Config, FixtureRequest, MonkeyPatch

from pytest_httpx._httpx_mock import HTTPXMock
from pytest_httpx._httpx_internals import IteratorStream
from pytest_httpx._options import _HTTPXMockOptions
from pytest_httpx.version import __version__

__all__ = (
    "HTTPXMock",
    "IteratorStream",
    "__version__",
)


@pytest.fixture
def httpx_mock(
    monkeypatch: MonkeyPatch,
    request: FixtureRequest,
) -> Generator[HTTPXMock, None, None]:
    options = {}
    for marker in request.node.iter_markers("httpx_mock"):
        options = marker.kwargs | options
    __tracebackhide__ = methodcaller("errisinstance", TypeError)
    options = _HTTPXMockOptions(**options)

    mock = HTTPXMock(options)

    # Mock synchronous requests
    real_handle_request = httpx.HTTPTransport.handle_request

    def mocked_handle_request(
        transport: httpx.HTTPTransport, request: httpx.Request
    ) -> httpx.Response:
        if options.should_mock(request):
            return mock._handle_request(transport, request)
        return real_handle_request(transport, request)

    monkeypatch.setattr(
        httpx.HTTPTransport,
        "handle_request",
        mocked_handle_request,
    )

    # Mock asynchronous requests
    real_handle_async_request = httpx.AsyncHTTPTransport.handle_async_request

    async def mocked_handle_async_request(
        transport: httpx.AsyncHTTPTransport, request: httpx.Request
    ) -> httpx.Response:
        if options.should_mock(request):
            return await mock._handle_async_request(transport, request)
        return await real_handle_async_request(transport, request)

    monkeypatch.setattr(
        httpx.AsyncHTTPTransport,
        "handle_async_request",
        mocked_handle_async_request,
    )

    yield mock
    try:
        mock._assert_options()
    finally:
        mock.reset()


def pytest_configure(config: Config) -> None:
    config.addinivalue_line(
        "markers",
        "httpx_mock(*, assert_all_responses_were_requested=True, assert_all_requests_were_expected=True, can_send_already_matched_responses=False, should_mock=lambda request: True): Configure httpx_mock fixture.",
    )
