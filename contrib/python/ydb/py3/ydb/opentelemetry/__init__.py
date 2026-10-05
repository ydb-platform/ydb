"""Public OpenTelemetry entrypoints for YDB.

For vendor-neutral tracing and metrics (custom backends, Noop, interfaces) see
:mod:`ydb.observability`. This module is a convenience wrapper that constructs
OpenTelemetry-backed backends and installs them via
:func:`ydb.observability.enable_tracing` / :func:`ydb.observability.enable_metrics`.
"""


def enable_tracing(tracer=None):
    """Enable OpenTelemetry trace context propagation and span creation for all YDB gRPC calls.

    Any previously installed tracing provider (custom or OTel) is replaced —
    calling this a second time is safe and simply swaps the tracer.

    Args:
        tracer: Optional OTel tracer to use. If not provided, the default tracer named
            ``ydb.sdk`` from the global tracer provider will be used.
    """
    try:
        from ydb.opentelemetry.plugin import _enable_tracing
    except ImportError:
        raise ImportError(
            "OpenTelemetry packages are required for tracing support. "
            "Install them with: pip install ydb[opentelemetry]"
        ) from None

    _enable_tracing(tracer)


def disable_tracing():
    """Disable YDB OpenTelemetry hooks and allow :func:`enable_tracing` to run again."""
    try:
        from ydb.opentelemetry.plugin import _disable_tracing
    except ImportError:
        return

    _disable_tracing()


def enable_metrics(meter_provider=None):
    """Enable OpenTelemetry metrics collection for YDB SDK client metrics.

    This call is **idempotent**: if metrics are already enabled, later calls do nothing
    (including passing a different ``meter_provider``). Call :func:`disable_metrics`
    first to reconfigure or turn instrumentation off.

    Args:
        meter_provider: Optional OTel meter provider to use. If not provided, the
            default meter named ``ydb.sdk`` from the global meter provider will be used.
    """
    try:
        from ydb.opentelemetry.metrics_plugin import _enable_metrics
    except ImportError:
        raise ImportError(
            "OpenTelemetry packages are required for metrics support. "
            "Install them with: pip install ydb[opentelemetry]"
        ) from None

    _enable_metrics(meter_provider)


def disable_metrics():
    """Disable YDB OpenTelemetry metrics collection and allow :func:`enable_metrics` to run again."""
    try:
        from ydb.opentelemetry.metrics_plugin import _disable_metrics
    except ImportError:
        return

    _disable_metrics()


_LAZY_PROVIDERS = {
    "OtelTracingProvider": ("ydb.opentelemetry.plugin", "OtelTracingProvider"),
    "OtelMetricsProvider": ("ydb.opentelemetry.metrics_plugin", "OtelMetricsProvider"),
}


def __getattr__(name):
    # Lazily expose the OTel backends so ``from ydb.opentelemetry import
    # OtelTracingProvider`` works without importing ``opentelemetry`` at module
    # load time. When OTel is missing, raise AttributeError (not ImportError) so
    # hasattr()/getattr(..., default) introspection behaves normally; the install
    # hint rides along in the message, and enable_tracing()/enable_metrics() keep
    # their own ImportError guidance for the primary entrypoints.
    target = _LAZY_PROVIDERS.get(name)
    if target is not None:
        module_name, attr = target
        try:
            module = __import__(module_name, fromlist=[attr])
        except ImportError:
            raise AttributeError(
                f"{name} requires the OpenTelemetry packages. " "Install them with: pip install ydb[opentelemetry]"
            ) from None
        return getattr(module, attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "OtelMetricsProvider",
    "OtelTracingProvider",
    "disable_metrics",
    "disable_tracing",
    "enable_metrics",
    "enable_tracing",
]
