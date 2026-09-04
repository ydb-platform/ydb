"""OpenTelemetry adapter for the YDB observability interface.

This module implements :class:`ydb.observability.TracingProvider` on top of the
``opentelemetry`` packages. The SDK core does not import it — the OTel
dependency is only pulled in when a user calls :func:`ydb.opentelemetry.enable_tracing`.
"""

from typing import Dict, Iterable, Optional, Tuple

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.propagate import inject
from opentelemetry.trace import StatusCode

from ydb import issues
from ydb.issues import StatusCode as YdbStatusCode
from ydb.observability import enable_tracing as _observability_enable
from ydb.observability import disable_tracing as _observability_disable

# YDB client transport StatusCode values (401xxx band) -> OTel error.type transport_error.
_TRANSPORT_STATUSES = frozenset(
    {
        YdbStatusCode.CONNECTION_LOST,
        YdbStatusCode.CONNECTION_FAILURE,
        YdbStatusCode.DEADLINE_EXCEEDED,
        YdbStatusCode.CLIENT_INTERNAL_ERROR,
        YdbStatusCode.UNIMPLEMENTED,
    }
)

_KIND_MAP = {
    "client": trace.SpanKind.CLIENT,
    "internal": trace.SpanKind.INTERNAL,
}


def _set_error_on_span(span, exception):
    if isinstance(exception, issues.Error) and exception.status is not None:
        span.set_attribute("db.response.status_code", exception.status.name)
        error_type = "transport_error" if exception.status in _TRANSPORT_STATUSES else "ydb_error"
    else:
        error_type = type(exception).__qualname__

    span.set_attribute("error.type", error_type)
    span.set_status(StatusCode.ERROR, str(exception))
    span.record_exception(exception)


class _AttachContext:
    """Make an OTel span the active context for a ``with`` block.

    When ``end_on_exit=True`` (default) the span is ended on exit — used for
    single-shot RPCs. When ``end_on_exit=False`` the span is only ended on
    exception — used for streaming RPCs where the result iterator owns ``end()``.
    """

    def __init__(self, span, end_on_exit):
        self._span = span
        self._end_on_exit = end_on_exit
        self._token = None

    def __enter__(self):
        ctx = trace.set_span_in_context(self._span._span)
        self._token = otel_context.attach(ctx)
        return self._span

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._token is not None:
            otel_context.detach(self._token)
            self._token = None
        if exc_val is not None:
            self._span.set_error(exc_val)
            self._span.end()
        elif self._end_on_exit:
            self._span.end()
        return False


class TracingSpan:
    """OpenTelemetry-backed :class:`ydb.observability.Span` implementation."""

    def __init__(self, span):
        self._span = span

    def set_error(self, exception):
        _set_error_on_span(self._span, exception)

    def set_attribute(self, key, value):
        self._span.set_attribute(key, value)

    def end(self):
        self._span.end()

    def attach_context(self, end_on_exit=True):
        return _AttachContext(self, end_on_exit)


class OtelTracingProvider:
    """Bridges the YDB observability interface to an OpenTelemetry tracer.

    Args:
        tracer: An OTel tracer. If not provided, the global provider's
            ``ydb.sdk`` tracer is used.
    """

    def __init__(self, tracer=None):
        self._tracer = tracer if tracer is not None else trace.get_tracer("ydb.sdk")

    def create_span(self, name, attributes=None, kind=None):
        span = self._tracer.start_span(
            name,
            kind=_KIND_MAP.get(kind, trace.SpanKind.CLIENT),
            attributes=attributes or {},
        )
        return TracingSpan(span)

    def get_trace_metadata(self) -> Iterable[Tuple[str, str]]:
        headers: Dict[str, str] = {}
        inject(headers)
        return tuple(headers.items())


def _enable_tracing(tracer: Optional[object] = None) -> None:
    """Install an :class:`OtelTracingProvider` (idempotent replace).

    Called by :func:`ydb.opentelemetry.enable_tracing`. Any previously
    registered provider — OTel or custom — is replaced.
    """
    _observability_enable(OtelTracingProvider(tracer))


def _disable_tracing() -> None:
    _observability_disable()
