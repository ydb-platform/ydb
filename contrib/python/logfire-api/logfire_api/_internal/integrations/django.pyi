from django.http import HttpRequest as HttpRequest, HttpResponse as HttpResponse
from logfire._internal.utils import maybe_capture_server_headers as maybe_capture_server_headers
from opentelemetry.trace import Span as Span
from typing import Any, Callable

def instrument_django(*, capture_headers: bool, is_sql_commentor_enabled: bool | None, excluded_urls: str | None, request_hook: Callable[[Span, HttpRequest], None] | None, response_hook: Callable[[Span, HttpRequest, HttpResponse], None] | None, **kwargs: Any) -> None:
    """Instrument the `django` module so that spans are automatically created for each web request.

    See the `Logfire.instrument_django` method for details.
    """
