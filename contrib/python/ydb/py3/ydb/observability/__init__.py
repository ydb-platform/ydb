"""Vendor-neutral observability entrypoints for the YDB SDK.

Users pick tracing and/or metrics backends and register them here:

.. code-block:: python

    from ydb.observability import enable_tracing, enable_metrics
    from ydb.opentelemetry import OtelTracingProvider

    enable_tracing(OtelTracingProvider())  # or any custom TracingProvider

The SDK itself never imports ``opentelemetry`` — until a backend is enabled,
every span is a :class:`~ydb.observability.tracing.NoopSpan` and every metric is
dropped by a no-op registry.
"""

from typing import List, Optional

from ydb.observability.tracing import (
    NoopSpan,
    NoopTracingProvider,
    Span,
    SpanName,
    TracingProvider,
    _registry,
    _tracing_build_info_tokens,
)
from ydb.observability.metrics import (
    MetricsProvider,
    _metrics_build_info_tokens,
    _reset_metrics_provider,
    _set_metrics_provider,
)


def enable_tracing(provider: TracingProvider) -> None:
    """Install *provider* as the active tracing backend.

    Calling this a second time replaces the previous provider — the old one
    stops receiving spans immediately. To turn tracing off again call
    :func:`disable_tracing`.
    """
    _registry.set_provider(provider)


def disable_tracing() -> None:
    """Reset the tracing backend to Noop.

    After this, :func:`enable_tracing` can be called again with any provider.
    """
    _registry.set_provider(None)


def get_active_provider() -> Optional[TracingProvider]:
    """Return the currently installed provider, or ``None`` if tracing is disabled."""
    return _registry.get_provider() if _registry.is_active() else None


def enable_metrics(provider: MetricsProvider) -> None:
    """Install *provider* as the active metrics backend.

    Calling this a second time replaces the previous backend. To turn metrics off
    again call :func:`disable_metrics`.
    """
    _set_metrics_provider(provider)


def disable_metrics() -> None:
    """Reset the metrics backend to the built-in no-op provider."""
    _reset_metrics_provider()


def sdk_build_info_tokens() -> List[str]:
    """All ``x-ydb-sdk-build-info`` feature tokens contributed by observability.

    Aggregated across observability features so the SDK build-info header advertises
    every capability the client has turned on: tracing contributes
    ``ydb-sdk-tracing/<v>`` while active, metrics contribute ``ydb-sdk-metrics/<v>``.
    """
    tokens: List[str] = []
    tokens.extend(_tracing_build_info_tokens())
    tokens.extend(_metrics_build_info_tokens())
    return tokens


__all__ = [
    "MetricsProvider",
    "NoopSpan",
    "NoopTracingProvider",
    "Span",
    "SpanName",
    "TracingProvider",
    "disable_metrics",
    "disable_tracing",
    "enable_metrics",
    "enable_tracing",
    "get_active_provider",
]
