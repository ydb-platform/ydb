from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from opentelemetry.context import Context
from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware
from opentelemetry.sdk.trace import Tracer as SDKTracer
from opentelemetry.trace import NonRecordingSpan, SpanKind, Tracer, TracerProvider
from opentelemetry.trace.propagation import get_current_span
from opentelemetry.util import types as otel_types

from logfire._internal.constants import log_level_attributes
from logfire._internal.utils import is_asgi_send_receive_span_name, maybe_capture_server_headers

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from typing import Any, Callable, Protocol, TypedDict

    from opentelemetry.trace import Span
    from typing_extensions import Unpack

    from logfire import Logfire

    Scope = dict[str, Any]
    Receive = Callable[[], Awaitable[dict[str, Any]]]
    Send = Callable[[dict[str, Any]], Awaitable[None]]

    class ASGIApp(Protocol):
        def __call__(self, scope: Scope, receive: Receive, send: Send) -> Awaitable[None]: ...

    Hook = Callable[[Span, dict[str, Any]], None]

    class ASGIInstrumentKwargs(TypedDict, total=False):
        excluded_urls: str | None
        default_span_details: Callable[[Scope], tuple[str, dict[str, Any]]]
        server_request_hook: Hook | None
        client_request_hook: Hook | None
        client_response_hook: Hook | None
        http_capture_headers_server_request: list[str] | None
        http_capture_headers_server_response: list[str] | None
        http_capture_headers_sanitize_fields: list[str] | None


def tweak_asgi_spans_tracer_provider(logfire_instance: Logfire, record_send_receive: bool) -> TracerProvider:
    """Return a TracerProvider that customizes ASGI send/receive spans.

    If record_send_receive is False, spans are filtered out.
    If record_send_receive is True, spans are created with debug log level.
    """
    tracer_provider = logfire_instance.config.get_tracer_provider()
    return TweakAsgiTracerProvider(tracer_provider, record_send_receive)


@dataclass
class TweakAsgiTracerProvider(TracerProvider):
    tracer_provider: TracerProvider
    record_send_receive: bool

    def get_tracer(self, *args: Any, **kwargs: Any) -> Tracer:
        return TweakAsgiSpansTracer(self.tracer_provider.get_tracer(*args, **kwargs), self.record_send_receive)


@dataclass
class TweakAsgiSpansTracer(Tracer):
    tracer: Tracer
    record_send_receive: bool

    def start_span(
        self,
        name: str,
        context: Context | None = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: otel_types.Attributes = None,
        *args: Any,
        **kwargs: Any,
    ) -> Span:
        if is_asgi_send_receive_span_name(name):
            if not self.record_send_receive:
                # These are the noisy spans we want to skip.
                # Create a no-op span with the same SpanContext as the current span.
                # This means that any spans created within will have the current span as their parent,
                # as if this span didn't exist at all.
                return NonRecordingSpan(get_current_span(context).get_span_context())

            # If we're recording send/receive spans, set the log level to debug
            attributes = {**(attributes or {}), **log_level_attributes('debug')}
            return self.tracer.start_span(name, context, kind, attributes, *args, **kwargs)

        return self.tracer.start_span(name, context, kind, attributes, *args, **kwargs)

    # This means that `with start_as_current_span(...):`
    # is roughly equivalent to `with use_span(start_span(...)):`
    start_as_current_span = SDKTracer.start_as_current_span


def instrument_asgi(
    logfire_instance: Logfire,
    app: ASGIApp,
    *,
    record_send_receive: bool = False,
    capture_headers: bool = False,
    **kwargs: Unpack[ASGIInstrumentKwargs],
) -> ASGIApp:
    """Instrument `app` so that spans are automatically created for each request.

    See the `Logfire.instrument_asgi` method for details.
    """
    maybe_capture_server_headers(capture_headers)
    return OpenTelemetryMiddleware(
        app,
        **{  # type: ignore
            'tracer_provider': tweak_asgi_spans_tracer_provider(logfire_instance, record_send_receive),
            'meter_provider': logfire_instance.config.get_meter_provider(),
            **kwargs,
        },
    )
