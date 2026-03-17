from __future__ import annotations

from typing import Any, Callable

import requests
from opentelemetry.sdk.trace import Span

try:
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
except ImportError:
    raise RuntimeError(
        '`logfire.instrument_requests()` requires the `opentelemetry-instrumentation-requests` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[requests]'"
    )


def instrument_requests(
    excluded_urls: str | None = None,
    request_hook: Callable[[Span, requests.PreparedRequest], None] | None = None,
    response_hook: Callable[[Span, requests.PreparedRequest, requests.Response], None] | None = None,
    **kwargs: Any,
) -> None:
    """Instrument the `requests` module so that spans are automatically created for each request.

    See the `Logfire.instrument_requests` method for details.
    """
    RequestsInstrumentor().instrument(
        excluded_urls=excluded_urls,
        request_hook=request_hook,
        response_hook=response_hook,
        **kwargs,
    )
