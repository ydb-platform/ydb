"""Telemetry for SDK Core. (unstable)

Nothing in this module should be considered stable. The API may change.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Type

from typing_extensions import Protocol

import temporalio.bridge.temporal_sdk_bridge


class Runtime:
    """Runtime for SDK Core."""

    @staticmethod
    def _raise_in_thread(thread_id: int, exc_type: Type[BaseException]) -> bool:
        """Internal helper for raising an exception in thread."""
        return temporalio.bridge.temporal_sdk_bridge.raise_in_thread(
            thread_id, exc_type
        )

    def __init__(self, *, telemetry: TelemetryConfig) -> None:
        """Create SDK Core runtime."""
        self._ref = temporalio.bridge.temporal_sdk_bridge.init_runtime(telemetry)

    def retrieve_buffered_metrics(self, durations_as_seconds: bool) -> Sequence[Any]:
        """Get buffered metrics."""
        return self._ref.retrieve_buffered_metrics(durations_as_seconds)

    def write_test_info_log(self, message: str, extra_data: str) -> None:
        """Write a test core log at INFO level."""
        self._ref.write_test_info_log(message, extra_data)

    def write_test_debug_log(self, message: str, extra_data: str) -> None:
        """Write a test core log at DEBUG level."""
        self._ref.write_test_debug_log(message, extra_data)


@dataclass(frozen=True)
class LoggingConfig:
    """Python representation of the Rust struct for logging config."""

    filter: str
    forward_to: Optional[Callable[[Sequence[BufferedLogEntry]], None]]


@dataclass(frozen=True)
class MetricsConfig:
    """Python representation of the Rust struct for metrics config."""

    opentelemetry: Optional[OpenTelemetryConfig]
    prometheus: Optional[PrometheusConfig]
    buffered_with_size: int
    attach_service_name: bool
    global_tags: Optional[Mapping[str, str]]
    metric_prefix: Optional[str]


@dataclass(frozen=True)
class OpenTelemetryConfig:
    """Python representation of the Rust struct for OpenTelemetry config."""

    url: str
    headers: Mapping[str, str]
    metric_periodicity_millis: Optional[int]
    metric_temporality_delta: bool
    durations_as_seconds: bool
    http: bool


@dataclass(frozen=True)
class PrometheusConfig:
    """Python representation of the Rust struct for Prometheus config."""

    bind_address: str
    counters_total_suffix: bool
    unit_suffix: bool
    durations_as_seconds: bool
    histogram_bucket_overrides: Optional[Mapping[str, Sequence[float]]] = None


@dataclass(frozen=True)
class TelemetryConfig:
    """Python representation of the Rust struct for telemetry config."""

    logging: Optional[LoggingConfig]
    metrics: Optional[MetricsConfig]


# WARNING: This must match Rust runtime::BufferedLogEntry
class BufferedLogEntry(Protocol):
    """A buffered log entry."""

    @property
    def target(self) -> str:
        """Target category for the log entry."""
        ...

    @property
    def message(self) -> str:
        """Log message."""
        ...

    @property
    def time(self) -> float:
        """Time as from ``time.time`` since Unix epoch."""
        ...

    @property
    def level(self) -> int:
        """Python log level, with trace as 9."""
        ...

    @property
    def fields(self) -> Dict[str, Any]:
        """Additional log entry fields.
        Requesting this property performs a conversion from the internal
        representation to the Python representation on every request. Therefore
        callers should store the result instead of repeatedly calling.

        Raises:
            Exception: If the internal representation cannot be converted. This
                should not happen and if it does it is considered a bug in the
                SDK and should be reported.
        """
        ...
