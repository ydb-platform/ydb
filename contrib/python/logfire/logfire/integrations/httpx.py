from __future__ import annotations

from collections.abc import Awaitable
from typing import Any, Callable, NamedTuple

import httpx
from opentelemetry.trace import Span

# TODO(Marcelo): When https://github.com/open-telemetry/opentelemetry-python-contrib/pull/3098/ gets merged,
# and the next version of `opentelemetry-instrumentation-httpx` is released, we can just do a reimport:
# from opentelemetry.instrumentation.httpx import RequestInfo as RequestInfo
# from opentelemetry.instrumentation.httpx import ResponseInfo as ResponseInfo
# from opentelemetry.instrumentation.httpx import RequestHook as RequestHook
# from opentelemetry.instrumentation.httpx import ResponseHook as ResponseHook


class RequestInfo(NamedTuple):
    """Information about an HTTP request.

    This is the second parameter passed to the `RequestHook` function.
    """

    method: bytes
    url: httpx.URL
    headers: httpx.Headers
    stream: httpx.SyncByteStream | httpx.AsyncByteStream | None
    extensions: dict[str, Any] | None


class ResponseInfo(NamedTuple):
    """Information about an HTTP response.

    This is the second parameter passed to the `ResponseHook` function.
    """

    status_code: int
    headers: httpx.Headers
    stream: httpx.SyncByteStream | httpx.AsyncByteStream | None
    extensions: dict[str, Any] | None


RequestHook = Callable[[Span, RequestInfo], None]
ResponseHook = Callable[[Span, RequestInfo, ResponseInfo], None]
AsyncRequestHook = Callable[[Span, RequestInfo], Awaitable[None]]
AsyncResponseHook = Callable[[Span, RequestInfo, ResponseInfo], Awaitable[None]]
