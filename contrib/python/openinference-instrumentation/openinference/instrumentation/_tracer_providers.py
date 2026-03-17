import os
from typing import Any, Optional

from opentelemetry.sdk.environment_variables import (
    OTEL_ATTRIBUTE_COUNT_LIMIT,
    OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT,
)
from opentelemetry.sdk.trace import SpanLimits
from opentelemetry.sdk.trace import TracerProvider as OTelTracerProvider

from ._tracers import OITracer
from .config import TraceConfig


class TracerProvider(OTelTracerProvider):
    def __init__(
        self,
        *args: Any,
        config: Optional[TraceConfig] = None,
        **kwargs: Any,
    ) -> None:
        if not _has_span_limits(*args, **kwargs):
            kwargs["span_limits"] = _create_span_limits_with_large_defaults()
        super().__init__(*args, **kwargs)
        self._oi_trace_config = config or TraceConfig()

    def get_tracer(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> OITracer:
        tracer = super().get_tracer(*args, **kwargs)
        return OITracer(tracer, config=self._oi_trace_config)


def _has_span_limits(*args: Any, **kwargs: Any) -> bool:
    """Check if SpanLimits is set in the arguments or keyword arguments."""
    for arg in args:
        if isinstance(arg, SpanLimits):
            return True
    for key, value in kwargs.items():
        if key == "span_limits" and isinstance(value, SpanLimits):
            return True
    return False


_DEFAULT_OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT = 10_000


def _create_span_limits_with_large_defaults() -> SpanLimits:
    """Create SpanLimits with large default attribute count limit.

    This function creates a SpanLimits instance with large default attribute count
    limit of 10,000, unless the user has explicitly set attribute count limits
    via environment variables.

    Returns:
        SpanLimits: A SpanLimits instance with max_attributes set according to
            the precedence order above.
    """
    # Check environment variables
    span_limit = os.getenv(OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT, "")
    general_limit = os.getenv(OTEL_ATTRIBUTE_COUNT_LIMIT, "")

    if span_limit.strip() or general_limit.strip():
        # User has set some limit via environment variables
        # Let SpanLimits() handle the precedence and parsing
        return SpanLimits()
    # No user configuration, use default for span attributes specifically
    return SpanLimits(max_span_attributes=_DEFAULT_OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT)
