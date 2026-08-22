"""Vendor-neutral tracing interfaces, Noop implementation and SDK helpers.

The YDB SDK talks to a small :class:`TracingProvider` interface here. Concrete
providers (OpenTelemetry in :mod:`ydb.opentelemetry`, or any custom one written
by the user) plug in via :func:`ydb.observability.enable_tracing`. Until a
provider is installed everything is a no-op — the SDK never depends on
``opentelemetry`` being importable.
"""

import enum
from typing import Any, Callable, ContextManager, Iterable, List, Optional, Protocol, Tuple

from ydb.observability import metrics as _metrics
from ydb.observability._endpoint import split_endpoint as _split_endpoint


class SpanName(str, enum.Enum):
    """Canonical span names used across the YDB SDK."""

    CREATE_SESSION = "ydb.CreateSession"
    EXECUTE_QUERY = "ydb.ExecuteQuery"
    BEGIN_TRANSACTION = "ydb.BeginTransaction"
    COMMIT = "ydb.Commit"
    ROLLBACK = "ydb.Rollback"
    DRIVER_INITIALIZE = "ydb.Driver.Initialize"
    RUN_WITH_RETRY = "ydb.RunWithRetry"
    TRY = "ydb.Try"


class Span(Protocol):
    """Minimal span surface the SDK relies on.

    Any custom provider must return objects that satisfy this interface.
    """

    def set_error(self, exception: BaseException) -> None: ...

    def set_attribute(self, key: str, value: Any) -> None: ...

    def end(self) -> None: ...

    def attach_context(self, end_on_exit: bool = True) -> ContextManager["Span"]:
        """Return a context manager that makes this span active for its block.

        With ``end_on_exit=True`` (default) the span is ended on exit — used for
        single-shot RPCs. With ``end_on_exit=False`` the span is only ended on
        exception — used for streaming RPCs where the result iterator owns
        ``end()``.
        """
        ...


class TracingProvider(Protocol):
    """Pluggable tracing backend.

    Implement this to wire the SDK into any tracing system. Only two methods
    are required: creating spans, and returning propagation metadata that
    should ride along on outgoing gRPC calls.
    """

    def create_span(
        self,
        name: str,
        attributes: Optional[dict] = None,
        kind: Optional[str] = None,
    ) -> Span: ...

    def get_trace_metadata(self) -> Iterable[Tuple[str, str]]:
        """Return ``(key, value)`` pairs to inject into outgoing RPC metadata."""
        ...


class _NoopCtx:
    __slots__ = ("_span",)

    def __init__(self, span):
        self._span = span

    def __enter__(self):
        return self._span

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class NoopSpan:
    """Span implementation used while no provider is enabled."""

    def set_error(self, exception):
        pass

    def set_attribute(self, key, value):
        pass

    def end(self):
        pass

    def attach_context(self, end_on_exit=True):
        return _NoopCtx(self)


class NoopTracingProvider:
    """Default provider — every span is a :class:`NoopSpan`, no metadata."""

    _SPAN = NoopSpan()

    def create_span(self, name, attributes=None, kind=None):
        return self._SPAN

    def get_trace_metadata(self):
        return ()


_NOOP_PROVIDER = NoopTracingProvider()


class _TracingRegistry:
    """Holds the currently active :class:`TracingProvider`.

    A single instance (:data:`_registry`) is shared across the SDK. Swapping
    the provider via :meth:`set_provider` is atomic from the caller's point
    of view: the next span will use the new provider.
    """

    def __init__(self) -> None:
        self._provider: TracingProvider = _NOOP_PROVIDER

    def is_active(self) -> bool:
        return self._provider is not _NOOP_PROVIDER

    def set_provider(self, provider: Optional[TracingProvider]) -> None:
        self._provider = provider if provider is not None else _NOOP_PROVIDER

    def get_provider(self) -> TracingProvider:
        return self._provider

    def create_span(self, name, attributes=None, kind=None) -> Span:
        return self._provider.create_span(name, attributes, kind=kind)

    def get_trace_metadata(self) -> Iterable[Tuple[str, str]]:
        return self._provider.get_trace_metadata()


_registry = _TracingRegistry()


def get_trace_metadata() -> List[Tuple[str, str]]:
    """Return tracing metadata for gRPC calls (empty list when no provider)."""
    return list(_registry.get_trace_metadata())


TRACING_SDK_BUILD_INFO = "ydb-sdk-tracing/0.1.0"


def _tracing_build_info_tokens() -> List[str]:
    """Tracing's contribution to the ``x-ydb-sdk-build-info`` header.

    Returns ``["ydb-sdk-tracing/0.1.0"]`` once a provider is installed, otherwise an
    empty list. Aggregated with other features by
    :func:`ydb.observability.sdk_build_info_tokens`.
    """
    return [TRACING_SDK_BUILD_INFO] if _registry.is_active() else []


def _build_ydb_attrs(driver_config, node_id=None, peer=None):
    host, port = _split_endpoint(getattr(driver_config, "endpoint", None))
    attrs = {
        "db.system.name": "ydb",
        "db.namespace": getattr(driver_config, "database", None) or "",
        "server.address": host,
        "server.port": port,
    }
    if peer is not None:
        address, port_, location = peer
        if address is not None:
            attrs["network.peer.address"] = address
        if port_ is not None:
            attrs["network.peer.port"] = int(port_)
        if location:
            attrs["ydb.node.dc"] = location
    if node_id is not None:
        attrs["ydb.node.id"] = node_id
    return attrs


def create_span(name, attributes=None, kind="internal"):
    """Create a span with no YDB-specific attributes (used for SDK-internal operations)."""
    return _registry.create_span(name, attributes=attributes, kind=kind).attach_context()


class _TelemetryContext:
    """Attach tracing context and metrics lifecycle for one SDK operation."""

    def __init__(self, telemetry, span_context, metrics_context):
        self._telemetry = telemetry
        self._span_context = span_context
        self._metrics_context = metrics_context

    def __enter__(self):
        self._metrics_context.__enter__()
        self._span_context.__enter__()
        return self._telemetry

    def __exit__(self, exc_type, exc_val, exc_tb):
        span_result = self._span_context.__exit__(exc_type, exc_val, exc_tb)
        metrics_result = self._metrics_context.__exit__(exc_type, exc_val, exc_tb)
        return bool(span_result or metrics_result)


class _TelemetryOperation:
    """Span-compatible facade that forwards lifecycle events to tracing and metrics."""

    def __init__(self, span, metrics):
        self._span = span
        self._metrics = metrics

    def set_error(self, exception):
        self._span.set_error(exception)
        self._metrics.set_error(exception)

    def set_attribute(self, key, value):
        self._span.set_attribute(key, value)
        self._metrics.set_attribute(key, value)

    def end(self):
        self._span.end()
        self._metrics.end()

    def attach_context(self, end_on_exit=True):
        return _TelemetryContext(
            self,
            self._span.attach_context(end_on_exit=end_on_exit),
            self._metrics.attach_context(end_on_exit=end_on_exit),
        )


def create_ydb_span(name, driver_config, node_id=None, kind=None, peer=None) -> Span:
    """Create telemetry for one user-visible YDB client operation.

    The returned object is span-compatible: when tracing is active it carries the
    standard YDB attributes, and when metrics are active the same object also drives
    the client operation metrics. When neither is active a shared :class:`NoopSpan`
    is returned so callers can use ``.attach_context()``, ``.set_attribute(...)``
    etc. unconditionally with zero overhead.
    """
    tracing_active = _registry.is_active()
    metrics_active = _metrics.is_metrics_enabled()
    if not tracing_active and not metrics_active:
        return NoopTracingProvider._SPAN

    if tracing_active:
        span = _registry.create_span(name, attributes=_build_ydb_attrs(driver_config, node_id, peer), kind=kind)
    else:
        span = NoopTracingProvider._SPAN

    metrics_attrs = _metrics._build_ydb_metrics_attrs(driver_config) if metrics_active else None
    metrics = _metrics.create_metrics_operation(name, metrics_attrs)
    return _TelemetryOperation(span, metrics)


def set_peer_attributes(span: Span, peer) -> None:
    """Fill in network.peer.* and ydb.node.dc on an existing span once the peer is known."""
    if peer is None:
        return
    address, port, location = peer
    if address is not None:
        span.set_attribute("network.peer.address", address)
    if port is not None:
        span.set_attribute("network.peer.port", int(port))
    if location:
        span.set_attribute("ydb.node.dc", location)


def span_finish_callback(span: Span) -> Callable[..., None]:
    """Return an on_finish callable that ends *span* when a streaming result iterator completes."""

    def _finish(exception=None):
        if exception is not None:
            span.set_error(exception)
        span.end()

    return _finish
