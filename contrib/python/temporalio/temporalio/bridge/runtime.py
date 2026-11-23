"""Telemetry for SDK Core. (unstable)

Nothing in this module should be considered stable. The API may change.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from typing_extensions import Protocol

import temporalio.bridge.temporal_sdk_bridge


class Runtime:
    """Runtime for SDK Core."""

    @staticmethod
    def _raise_in_thread(thread_id: int, exc_type: type[BaseException]) -> bool:
        """Internal helper for raising an exception in thread."""
        return temporalio.bridge.temporal_sdk_bridge.raise_in_thread(
            thread_id, exc_type
        )

    def __init__(self, *, options: RuntimeOptions) -> None:
        """Create SDK Core runtime."""
        self._ref = temporalio.bridge.temporal_sdk_bridge.init_runtime(options)

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
    forward_to: Callable[[Sequence[BufferedLogEntry]], None] | None
    format: str | None


@dataclass(frozen=True)
class MetricsConfig:
    """Python representation of the Rust struct for metrics config."""

    opentelemetry: OpenTelemetryConfig | None
    prometheus: PrometheusConfig | None
    buffered_with_size: int
    attach_service_name: bool
    global_tags: Mapping[str, str] | None
    metric_prefix: str | None


@dataclass(frozen=True)
class OpenTelemetryConfig:
    """Python representation of the Rust struct for OpenTelemetry config."""

    url: str
    headers: Mapping[str, str]
    metric_periodicity_millis: int | None
    metric_temporality_delta: bool
    durations_as_seconds: bool
    http: bool
    histogram_bucket_overrides: Mapping[str, Sequence[float]] | None = None


@dataclass(frozen=True)
class PrometheusConfig:
    """Python representation of the Rust struct for Prometheus config."""

    bind_address: str
    counters_total_suffix: bool
    unit_suffix: bool
    durations_as_seconds: bool
    histogram_bucket_overrides: Mapping[str, Sequence[float]] | None = None


@dataclass(frozen=True)
class TelemetryConfig:
    """Python representation of the Rust struct for telemetry config."""

    logging: LoggingConfig | None
    metrics: MetricsConfig | None


@dataclass(frozen=True)
class RuntimeOptions:
    """Python representation of the Rust struct for runtime options."""

    telemetry: TelemetryConfig
    worker_heartbeat_interval_millis: int | None = 60_000  # 60s
    disable_environment_info: bool = False


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
    def fields(self) -> dict[str, Any]:
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
