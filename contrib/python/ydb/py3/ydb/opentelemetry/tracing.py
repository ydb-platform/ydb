"""Backward-compatible re-exports.

The tracing interfaces, Noop implementation and SDK helpers now live in
:mod:`ydb.observability.tracing`. This module is preserved so existing imports
like ``from ydb.opentelemetry.tracing import SpanName, create_ydb_span``
keep working, but new code should import from :mod:`ydb.observability`.
"""

from ydb.observability.tracing import (  # noqa: F401
    NoopSpan,
    NoopTracingProvider,
    Span,
    SpanName,
    TracingProvider,
    _NoopCtx,
    _build_ydb_attrs,
    _registry,
    _split_endpoint,
    create_span,
    create_ydb_span,
    get_trace_metadata,
    set_peer_attributes,
    span_finish_callback,
)

_NoopSpan = NoopSpan
_NOOP_SPAN = NoopTracingProvider._SPAN
