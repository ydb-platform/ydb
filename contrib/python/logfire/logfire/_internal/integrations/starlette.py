from __future__ import annotations

from typing import Any

from starlette.applications import Starlette

try:
    from opentelemetry.instrumentation.asgi.types import ClientRequestHook, ClientResponseHook, ServerRequestHook
    from opentelemetry.instrumentation.starlette import StarletteInstrumentor
except ImportError:
    raise RuntimeError(
        '`logfire.instrument_starlette()` requires the `opentelemetry-instrumentation-starlette` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[starlette]'"
    )

from logfire import Logfire
from logfire._internal.integrations.asgi import tweak_asgi_spans_tracer_provider
from logfire._internal.utils import maybe_capture_server_headers


def instrument_starlette(
    logfire_instance: Logfire,
    app: Starlette,
    *,
    record_send_receive: bool = False,
    capture_headers: bool = False,
    server_request_hook: ServerRequestHook | None = None,
    client_request_hook: ClientRequestHook | None = None,
    client_response_hook: ClientResponseHook | None = None,
    **kwargs: Any,
):
    """Instrument `app` so that spans are automatically created for each request.

    See the `Logfire.instrument_starlette` method for details.
    """
    maybe_capture_server_headers(capture_headers)
    StarletteInstrumentor().instrument_app(
        app,
        server_request_hook=server_request_hook,
        client_request_hook=client_request_hook,
        client_response_hook=client_response_hook,
        **{  # type: ignore
            'tracer_provider': tweak_asgi_spans_tracer_provider(logfire_instance, record_send_receive),
            'meter_provider': logfire_instance.config.get_meter_provider(),
            **kwargs,
        },
    )
