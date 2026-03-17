import httpx
from collections.abc import Awaitable
from opentelemetry.trace import Span
from typing import Any, Callable, NamedTuple

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
