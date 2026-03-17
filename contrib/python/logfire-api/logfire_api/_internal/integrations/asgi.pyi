from collections.abc import Awaitable
from dataclasses import dataclass
from logfire import Logfire as Logfire
from logfire._internal.constants import log_level_attributes as log_level_attributes
from logfire._internal.utils import is_asgi_send_receive_span_name as is_asgi_send_receive_span_name, maybe_capture_server_headers as maybe_capture_server_headers
from opentelemetry.context import Context
from opentelemetry.trace import Span, SpanKind, Tracer, TracerProvider
from opentelemetry.util import types as otel_types
from typing import Any, Callable, Protocol, TypedDict
from typing_extensions import Unpack

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

@dataclass
class TweakAsgiTracerProvider(TracerProvider):
    tracer_provider: TracerProvider
    record_send_receive: bool
    def get_tracer(self, *args: Any, **kwargs: Any) -> Tracer: ...

@dataclass
class TweakAsgiSpansTracer(Tracer):
    tracer: Tracer
    record_send_receive: bool
    def start_span(self, name: str, context: Context | None = None, kind: SpanKind = ..., attributes: otel_types.Attributes = None, *args: Any, **kwargs: Any) -> Span: ...
    start_as_current_span = ...

def instrument_asgi(logfire_instance: Logfire, app: ASGIApp, *, record_send_receive: bool = False, capture_headers: bool = False, **kwargs: Unpack[ASGIInstrumentKwargs]) -> ASGIApp:
    """Instrument `app` so that spans are automatically created for each request.

    See the `Logfire.instrument_asgi` method for details.
    """
