"""OpenTelemetry metrics adapter for the YDB observability interface.

Implements :class:`ydb.observability.MetricsProvider` on top of the ``opentelemetry``
packages. The SDK core does not import it — the OTel dependency is only pulled in when a
user calls :func:`ydb.opentelemetry.enable_metrics`.
"""

from typing import Any, Dict, Optional

from opentelemetry import metrics as otel_metrics
from opentelemetry.metrics import (
    CallbackOptions,
    Histogram,
    Meter,
    MeterProvider,
    Observation,
)

from ydb.observability import enable_metrics as _observability_enable_metrics
from ydb.observability import disable_metrics as _observability_disable_metrics
from ydb.observability.metrics import (
    CLIENT_OPERATION_DURATION,
    CLIENT_OPERATION_FAILED,
    QUERY_SESSION_COUNT,
    QUERY_SESSION_CREATE_TIME,
    QUERY_SESSION_MAX,
    QUERY_SESSION_MIN,
    QUERY_SESSION_PENDING_REQUESTS,
    QUERY_SESSION_TIMEOUTS,
    RETRY_ATTEMPTS,
    RETRY_DURATION,
    ATTEMPT_BUCKETS,
    DURATION_BUCKETS_SECONDS,
    GaugeCallback,
    RETRY_DURATION_BUCKETS_SECONDS,
)

_meter: Optional[Meter] = None

# Unit/description for the asynchronous gauges the SDK registers via observe_gauge.
_GAUGE_META = {
    QUERY_SESSION_COUNT: ("{connection}", "Number of open YDB query sessions."),
    QUERY_SESSION_MAX: ("{connection}", "Maximum configured number of YDB query sessions."),
    QUERY_SESSION_MIN: ("{connection}", "Minimum configured number of YDB query sessions."),
}


class OtelMetricsProvider:
    """OpenTelemetry-backed :class:`ydb.observability.MetricsProvider`.

    Push instruments (histograms, counters) are created eagerly with the SDK's
    units/buckets; asynchronous gauges are created lazily via :meth:`observe_gauge`.
    """

    def __init__(self, meter: Meter) -> None:
        self._meter = meter
        self._histograms: Dict[str, Histogram] = {
            CLIENT_OPERATION_DURATION: _create_histogram(
                meter,
                CLIENT_OPERATION_DURATION,
                unit="s",
                description="Duration of YDB client operations.",
                bucket_boundaries=DURATION_BUCKETS_SECONDS,
            ),
            QUERY_SESSION_CREATE_TIME: _create_histogram(
                meter,
                QUERY_SESSION_CREATE_TIME,
                unit="s",
                description="Duration of YDB query session creation.",
                bucket_boundaries=DURATION_BUCKETS_SECONDS,
            ),
            RETRY_DURATION: _create_histogram(
                meter,
                RETRY_DURATION,
                unit="s",
                description=(
                    "Total user-visible duration of a logical operation executed through the retry policy, "
                    "including all attempts and back-off delays."
                ),
                bucket_boundaries=RETRY_DURATION_BUCKETS_SECONDS,
            ),
            RETRY_ATTEMPTS: _create_histogram(
                meter,
                RETRY_ATTEMPTS,
                unit="{attempt}",
                description=(
                    "Total number of attempts performed by the retry policy for one logical operation. "
                    "A value of 1 means the operation succeeded on the first try."
                ),
                bucket_boundaries=ATTEMPT_BUCKETS,
            ),
        }
        self._counters: Dict[str, Any] = {
            CLIENT_OPERATION_FAILED: meter.create_counter(
                CLIENT_OPERATION_FAILED,
                unit="{command}",
                description="Number of failed YDB client operations.",
            ),
            QUERY_SESSION_TIMEOUTS: meter.create_counter(
                QUERY_SESSION_TIMEOUTS,
                unit="{connection}",
                description="Number of YDB query session acquisition timeouts.",
            ),
            QUERY_SESSION_PENDING_REQUESTS: meter.create_up_down_counter(
                QUERY_SESSION_PENDING_REQUESTS,
                unit="{request}",
                description="Number of requests waiting for a YDB query session.",
            ),
        }

    def record(self, name: str, value: float, attributes: Optional[Dict[str, Any]] = None) -> None:
        instrument = self._histograms.get(name)
        if instrument is not None:
            instrument.record(value, attributes=attributes or {})

    def add(self, name: str, value: int, attributes: Optional[Dict[str, Any]] = None) -> None:
        instrument = self._counters.get(name)
        if instrument is not None:
            instrument.add(value, attributes=attributes or {})

    def observe_gauge(self, name: str, callback: GaugeCallback) -> None:
        unit, description = _GAUGE_META.get(name, ("", ""))

        def _otel_callback(_: CallbackOptions):
            return [Observation(value, attributes=attrs) for value, attrs in callback()]

        self._meter.create_observable_up_down_counter(
            name,
            callbacks=[_otel_callback],
            unit=unit,
            description=description,
        )


def _enable_metrics(meter_provider: Optional[MeterProvider]) -> None:
    """Build an :class:`OtelMetricsProvider` from an OTel MeterProvider and install it.

    Called by :func:`ydb.opentelemetry.enable_metrics`. Idempotent: if metrics are
    already enabled the call is a no-op (re-registering OpenTelemetry observable
    callbacks would double-count). Call :func:`ydb.opentelemetry.disable_metrics`
    first to reconfigure.
    """
    global _meter

    if _meter is not None:
        return

    if meter_provider is None:
        _meter = otel_metrics.get_meter("ydb.sdk")
    elif hasattr(meter_provider, "get_meter"):
        _meter = meter_provider.get_meter("ydb.sdk")
    else:
        raise TypeError("meter_provider must be an OpenTelemetry MeterProvider")

    _observability_enable_metrics(OtelMetricsProvider(_meter))


def _disable_metrics() -> None:
    global _meter

    _observability_disable_metrics()
    _meter = None


def _create_histogram(
    meter: Meter,
    name: str,
    unit: str,
    description: str,
    bucket_boundaries,
) -> Histogram:
    """Create a histogram with bucket advice when the installed OpenTelemetry SDK supports it."""
    try:
        return meter.create_histogram(
            name,
            unit=unit,
            description=description,
            explicit_bucket_boundaries_advisory=bucket_boundaries,
        )
    except TypeError as e:
        if "explicit_bucket_boundaries_advisory" not in str(e):
            raise
        return meter.create_histogram(
            name,
            unit=unit,
            description=description,
        )
