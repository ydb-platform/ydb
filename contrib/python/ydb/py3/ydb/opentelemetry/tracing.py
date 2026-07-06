"""Internal SDK tracing helpers and registry."""

import enum
from typing import Optional, Tuple


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


class _NoopCtx:
    __slots__ = ("_span",)

    def __init__(self, span):
        self._span = span

    def __enter__(self):
        return self._span

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class _NoopSpan:
    """Returned by create_ydb_span when tracing is disabled."""

    def set_error(self, exception):
        pass

    def set_attribute(self, key, value):
        pass

    def end(self):
        pass

    def attach_context(self, end_on_exit=True):
        return _NoopCtx(self)


_NOOP_SPAN = _NoopSpan()


class OtelTracingRegistry:
    """Singleton registry for OpenTelemetry tracing.

    By default everything is no-op until :func:`~ydb.opentelemetry.enable_tracing` is called.
    """

    def __init__(self):
        self._metadata_hook = None
        self._create_span_func = None

    def is_active(self) -> bool:
        return self._create_span_func is not None

    def create_span(self, name, attributes=None, kind=None):
        if self._create_span_func is None:
            return _NOOP_SPAN
        return self._create_span_func(name, attributes, kind=kind)

    def get_trace_metadata(self):
        if self._metadata_hook is not None:
            return self._metadata_hook()
        return []

    def set_metadata_hook(self, hook):
        self._metadata_hook = hook

    def set_create_span(self, func):
        self._create_span_func = func


_registry = OtelTracingRegistry()


def get_trace_metadata():
    """Return tracing metadata for gRPC calls."""
    return _registry.get_trace_metadata()


def _split_endpoint(endpoint: Optional[str]) -> Tuple[str, int]:
    ep = endpoint or ""
    if ep.startswith("grpcs://"):
        ep = ep[len("grpcs://") :]
    elif ep.startswith("grpc://"):
        ep = ep[len("grpc://") :]

    if ep.startswith("["):
        close = ep.find("]")
        if close != -1 and len(ep) > close + 1 and ep[close + 1] == ":":
            host = ep[: close + 1]
            port_s = ep[close + 2 :]
            return host, int(port_s) if port_s.isdigit() else 0

    host, sep, port_s = ep.rpartition(":")
    if not sep:
        return ep, 0
    return host, int(port_s) if port_s.isdigit() else 0


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


def create_ydb_span(name, driver_config, node_id=None, kind=None, peer=None):
    """Create a span pre-filled with standard YDB attributes."""
    if not _registry.is_active():
        return _NOOP_SPAN
    attrs = _build_ydb_attrs(driver_config, node_id, peer)
    return _registry.create_span(name, attributes=attrs, kind=kind)


def set_peer_attributes(span, peer):
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


def span_finish_callback(span):
    """Return an on_finish callable that ends *span* when a streaming result iterator completes."""

    def _finish(exception=None):
        if exception is not None:
            span.set_error(exception)
        span.end()

    return _finish
