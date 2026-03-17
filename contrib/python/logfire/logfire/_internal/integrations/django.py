from __future__ import annotations

from typing import Any, Callable

from django.http import HttpRequest, HttpResponse
from opentelemetry.trace import Span

from logfire._internal.utils import maybe_capture_server_headers

try:
    from opentelemetry.instrumentation.django import DjangoInstrumentor
except ImportError:
    raise RuntimeError(
        '`logfire.instrument_django()` requires the `opentelemetry-instrumentation-django` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[django]'"
    )


def instrument_django(
    *,
    capture_headers: bool,
    is_sql_commentor_enabled: bool | None,
    excluded_urls: str | None,
    request_hook: Callable[[Span, HttpRequest], None] | None,
    response_hook: Callable[[Span, HttpRequest, HttpResponse], None] | None,
    **kwargs: Any,
) -> None:
    """Instrument the `django` module so that spans are automatically created for each web request.

    See the `Logfire.instrument_django` method for details.
    """
    maybe_capture_server_headers(capture_headers)
    DjangoInstrumentor().instrument(
        excluded_urls=excluded_urls,
        is_sql_commentor_enabled=is_sql_commentor_enabled,
        request_hook=request_hook,
        response_hook=response_hook,
        **kwargs,
    )
