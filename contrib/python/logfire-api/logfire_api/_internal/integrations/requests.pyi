import requests
from opentelemetry.sdk.trace import Span as Span
from typing import Any, Callable

def instrument_requests(excluded_urls: str | None = None, request_hook: Callable[[Span, requests.PreparedRequest], None] | None = None, response_hook: Callable[[Span, requests.PreparedRequest, requests.Response], None] | None = None, **kwargs: Any) -> None:
    """Instrument the `requests` module so that spans are automatically created for each request.

    See the `Logfire.instrument_requests` method for details.
    """
