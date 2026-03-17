from __future__ import annotations

from typing import TYPE_CHECKING, Any

try:
    from opentelemetry.instrumentation.wsgi import OpenTelemetryMiddleware
except ImportError:
    raise RuntimeError(
        '`logfire.instrument_wsgi()` requires the `opentelemetry-instrumentation-wsgi` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[wsgi]'"
    )

if TYPE_CHECKING:
    from wsgiref.types import WSGIApplication

from logfire._internal.utils import maybe_capture_server_headers
from logfire.integrations.wsgi import RequestHook, ResponseHook


def instrument_wsgi(
    app: WSGIApplication,
    *,
    capture_headers: bool = False,
    request_hook: RequestHook | None = None,
    response_hook: ResponseHook | None = None,
    **kwargs: Any,
) -> WSGIApplication:
    """Instrument `app` so that spans are automatically created for each request.

    See the `Logfire.instrument_wsgi` method for details.
    """
    maybe_capture_server_headers(capture_headers)
    return OpenTelemetryMiddleware(app, request_hook=request_hook, response_hook=response_hook, **kwargs)
