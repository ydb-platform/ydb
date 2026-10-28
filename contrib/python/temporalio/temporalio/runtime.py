"""Runtime for clients and workers."""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import (
    ClassVar,
    Generic,
    NewType,
    TypeVar,
)

from typing_extensions import Protocol, Self

import temporalio.bridge.metric
import temporalio.bridge.runtime
import temporalio.common


class _RuntimeRef:
    def __init__(
        self,
    ) -> None:
        self._default_runtime: Runtime | None = None
        self._prevent_default = False

    def default(self) -> Runtime:
        if not self._default_runtime:
            if self._prevent_default:
                raise RuntimeError(
                    "Cannot create default Runtime after Runtime.prevent_default has been called"
                )
            self._default_runtime = Runtime(telemetry=TelemetryConfig())
        return self._default_runtime

    def prevent_default(self):
        if self._default_runtime:
            raise RuntimeError(
                "Runtime.prevent_default called after default runtime has been created or set"
            )
        self._prevent_default = True

    def set_default(
        self, runtime: Runtime, *, error_if_already_set: bool = True
    ) -> None:
        if self._default_runtime and error_if_already_set:
            raise RuntimeError("Runtime default already set")

        self._default_runtime = runtime


_runtime_ref: _RuntimeRef = _RuntimeRef()


class Runtime:
    """Runtime for Temporal Python SDK.

    Most users are encouraged to use :py:meth:`default`. It can be set with
    :py:meth:`set_default`. Every time a new runtime is created, a new internal
    thread pool is created.

    Runtimes do not work across forks. Advanced users should consider using
    :py:meth:`prevent_default` and :py:meth:`set_default` to ensure each
    fork creates it's own runtime.

    """

    @classmethod
    def default(cls) -> Runtime:
        """Get the default runtime, creating if not already created. If :py:meth:`prevent_default`
        is called before this method it will raise a RuntimeError instead of creating a default
        runtime.

        If the default runtime needs to be different, it should be done with
        :py:meth:`set_default` before this is called or ever used.

        Returns:
            The default runtime.
        """
        global _runtime_ref
        return _runtime_ref.default()

    @classmethod
    def prevent_default(cls):
        """Prevent :py:meth:`default` from lazily creating a :py:class:`Runtime`.

        Raises a RuntimeError if a default :py:class:`Runtime` has already been created.

        Explicitly setting a default runtime with :py:meth:`set_default` bypasses this setting and
        future calls to :py:meth:`default` will return the provided runtime.
        """
        global _runtime_ref
        _runtime_ref.prevent_default()

    @staticmethod
    def set_default(runtime: Runtime, *, error_if_already_set: bool = True) -> None:
        """Set the default runtime to the given runtime.

        This should be called before any Temporal client is created, but can
        change the existing one. Any clients and workers created with the
        previous runtime will stay on that runtime.

        Args:
            runtime: The runtime to set.
            error_if_already_set: If True and default is already set, this will
                raise a RuntimeError.
        """
        global _runtime_ref
        _runtime_ref.set_default(runtime, error_if_already_set=error_if_already_set)

    def __init__(
        self,
        *,
        telemetry: TelemetryConfig,
        worker_heartbeat_interval: timedelta | None = timedelta(seconds=60),
        disable_environment_info: bool = False,
    ) -> None:
        """Create a runtime with the provided configuration.

        Each new runtime creates a new internal thread pool, so use sparingly.

        Args:
            telemetry: Telemetry configuration when not supplying
                ``runtime_options``.
            worker_heartbeat_interval: Interval for worker heartbeats. ``None``
                disables heartbeating. Interval must be between 1s and 60s.
            disable_environment_info: Whether to omit runtime, hosting, and
                platform information from worker heartbeats.

        Raises:
            ValueError: If both ```runtime_options`` is a negative value.
        """
        if worker_heartbeat_interval is None:
            heartbeat_millis = None
        else:
            if worker_heartbeat_interval <= timedelta(0):
                raise ValueError("worker_heartbeat_interval must be positive")
            heartbeat_millis = int(worker_heartbeat_interval.total_seconds() * 1000)

        runtime_options = temporalio.bridge.runtime.RuntimeOptions(
            telemetry=telemetry._to_bridge_config(),
            worker_heartbeat_interval_millis=heartbeat_millis,
            disable_environment_info=disable_environment_info,
        )

        self._core_runtime = temporalio.bridge.runtime.Runtime(options=runtime_options)
        if isinstance(telemetry.metrics, MetricBuffer):
            telemetry.metrics._runtime = self
        core_meter = temporalio.bridge.metric.MetricMeter.create(self._core_runtime)
        if not core_meter:
            self._metric_meter = temporalio.common.MetricMeter.noop
        else:
            self._metric_meter = _MetricMeter(core_meter, core_meter.default_attributes)

    @property
    def metric_meter(self) -> temporalio.common.MetricMeter:
        """Metric meter for this runtime. This is a no-op metric meter if no
        metrics were configured.
        """
        return self._metric_meter


@dataclass
class TelemetryFilter:
    """Filter for telemetry use."""

    core_level: str
    """Level for Core. Can be ``ERROR``, ``WARN``, ``INFO``, ``DEBUG``, or
    ``TRACE``.
    """

    other_level: str
    """Level for non-Core. Can be ``ERROR``, ``WARN``, ``INFO``, ``DEBUG``, or
    ``TRACE``.
    """

    def formatted(self) -> str:
        """Return a formatted form of this filter."""
        # We intentionally aren't using __str__ or __format__ so they can keep
        # their original dataclass impls
        targets = [
            "temporalio_common",
            "temporalio_sdk_core",
            "temporalio_client",
            "temporalio_sdk",
            "temporal_sdk_bridge",
        ]
        parts = [self.other_level]
        parts.extend(f"{target}={self.core_level}" for target in targets)
        return ",".join(parts)


class LoggingFormat(Enum):
    """Format for Core logs written to the console."""

    COMPACT = "compact"
    PRETTY = "pretty"
    JSON = "json"


@dataclass(frozen=True)
class LoggingConfig:
    """Configuration for runtime logging."""

    filter: TelemetryFilter | str
    """Filter for logging. Can use :py:class:`TelemetryFilter` or raw string."""

    forwarding: LogForwardingConfig | None = None
    """If present, Core logger messages will be forwarded to a Python logger.
    See the :py:class:`LogForwardingConfig` docs for more info.
    """

    format: LoggingFormat | None = None
    """Format for Core logs written to the console. This is ignored when
    :py:attr:`forwarding` is set. If unset, this defaults to compact output.
    """

    default: ClassVar[LoggingConfig]
    """Default logging configuration of Core WARN level and other ERROR
    level.
    """

    def _to_bridge_config(self) -> temporalio.bridge.runtime.LoggingConfig:
        return temporalio.bridge.runtime.LoggingConfig(
            filter=self.filter
            if isinstance(self.filter, str)
            else self.filter.formatted(),
            forward_to=None if not self.forwarding else self.forwarding._on_logs,
            format=None if not self.format else self.format.value,
        )


LoggingConfig.default = LoggingConfig(
    filter=TelemetryFilter(core_level="WARN", other_level="ERROR")
)

_module_start_time = time.time()


@dataclass
class LogForwardingConfig:
    """Configuration for log forwarding from Core.

    Configuring this will send logs from Core to the given Python logger. By
    default, log timestamps are overwritten and internally throttled/buffered
    for a few milliseconds to prevent overloading Python. This means those log
    records may have a time in the past and technically may appear out of order
    with Python-originated log messages by a few milliseconds.

    If for some reason lots of logs occur within the buffered time (i.e.
    thousands), they may be sent earlier. Users are discouraged from using this
    with ``TRACE`` Core logging.

    All log records produced have a ``temporal_log`` attribute that contains a
    representation of the Core log. This representation has a ``fields``
    attribute which has arbitrary extra data from Core. By default a string
    representation of this extra ``fields`` attribute is appended to the
    message.
    """

    logger: logging.Logger
    """Core logger messages will be sent to this logger."""

    append_target_to_name: bool = True
    """If true, the default, the target is appended to the name."""

    prepend_target_on_message: bool = True
    """If true, the default, the target is appended to the name."""

    overwrite_log_record_time: bool = True
    """If true, the default, the log record time is overwritten with the core
    log time."""

    append_log_fields_to_message: bool = True
    """If true, the default, the extra fields dict is appended to the
    message."""

    def _on_logs(
        self, logs: Sequence[temporalio.bridge.runtime.BufferedLogEntry]
    ) -> None:
        for log in logs:
            # Don't go further if not enabled
            level = log.level
            if not self.logger.isEnabledFor(level):
                continue

            # Create the record
            name = self.logger.name
            if self.append_target_to_name:
                name += f"-sdk_core::{log.target}"
            message = log.message
            if self.prepend_target_on_message:
                message = f"[sdk_core::{log.target}] {message}"
            if self.append_log_fields_to_message:
                # Swallow error converting fields (should never happen, but
                # just in case)
                try:
                    message += f" {log.fields}"
                except:
                    pass
            record = self.logger.makeRecord(
                name,
                level,
                "(sdk-core)",
                0,
                message,
                (),
                None,
                "(sdk-core)",
                {"temporal_log": log},
                None,
            )
            if self.overwrite_log_record_time:
                record.created = log.time
                record.msecs = (record.created - int(record.created)) * 1000
                # We can't access logging module's start time and it's not worth
                # doing difference math to get relative time right here, so
                # we'll make time relative to _our_ module's start time
                self.relativeCreated = (record.created - _module_start_time) * 1000  # type: ignore[reportUninitializedInstanceVariable]
            # Log the record
            self.logger.handle(record)


class OpenTelemetryMetricTemporality(Enum):
    """Temporality for OpenTelemetry metrics."""

    CUMULATIVE = 1
    DELTA = 2


@dataclass(frozen=True)
class OpenTelemetryConfig:
    """Configuration for OpenTelemetry collector.

    Attributes:
        url: URL of the OpenTelemetry collector endpoint (e.g.
            ``"http://localhost:4317"`` for gRPC or
            ``"http://localhost:4318/v1/metrics"`` for HTTP).
        headers: Optional headers to include with each export request.
            Useful for authentication tokens or routing metadata.
        metric_periodicity: How often metrics are exported to the collector.
            Defaults to 1s (set by sdk-core) when ``None``.
        metric_temporality: Whether metrics are exported as cumulative
            or delta values. Defaults to ``CUMULATIVE``.
        durations_as_seconds: If ``True``, export duration metrics as
            floating-point seconds instead of integer milliseconds.
            Defaults to ``False``.
        http: If ``True``, use HTTP/protobuf transport instead of gRPC.
            When enabled, the ``url`` should point to the HTTP endpoint
            (e.g. ``"http://localhost:4318/v1/metrics"``).
            Defaults to ``False`` (gRPC).
        histogram_bucket_overrides: Override the default histogram bucket
            boundaries for specific metrics. Keys are metric names and
            values are sequences of bucket boundaries (e.g.
            ``{"workflow_task_schedule_to_start_latency": [0.01, 0.05, 0.1, 0.5, 1.0, 5.0]}``).
    """

    url: str
    headers: Mapping[str, str] | None = None
    metric_periodicity: timedelta | None = None
    metric_temporality: OpenTelemetryMetricTemporality = (
        OpenTelemetryMetricTemporality.CUMULATIVE
    )
    durations_as_seconds: bool = False
    http: bool = False
    histogram_bucket_overrides: Mapping[str, Sequence[float]] | None = None

    def _to_bridge_config(self) -> temporalio.bridge.runtime.OpenTelemetryConfig:
        return temporalio.bridge.runtime.OpenTelemetryConfig(
            url=self.url,
            headers=self.headers or {},
            metric_periodicity_millis=(
                None
                if not self.metric_periodicity
                else round(self.metric_periodicity.total_seconds() * 1000)
            ),
            metric_temporality_delta=(
                self.metric_temporality == OpenTelemetryMetricTemporality.DELTA
            ),
            durations_as_seconds=self.durations_as_seconds,
            http=self.http,
            histogram_bucket_overrides=self.histogram_bucket_overrides,
        )


@dataclass(frozen=True)
class PrometheusConfig:
    """Configuration for Prometheus metrics endpoint.

    Starts an HTTP server on the given address that exposes a ``/metrics``
    endpoint for Prometheus scraping.

    Attributes:
        bind_address: Address to bind the metrics HTTP server to (e.g.
            ``"0.0.0.0:9000"`` or ``"127.0.0.1:9090"``). Prometheus
            will scrape ``http://<bind_address>/metrics``.
        counters_total_suffix: If ``True``, append ``_total`` suffix to
            counter metric names, following the OpenMetrics convention.
            Defaults to ``False``.
        unit_suffix: If ``True``, append unit suffixes (e.g. ``_seconds``,
            ``_bytes``) to metric names. Defaults to ``False``.
        durations_as_seconds: If ``True``, report duration metrics as
            floating-point seconds instead of integer milliseconds.
            Defaults to ``False``.
        histogram_bucket_overrides: Override the default histogram bucket
            boundaries for specific metrics. Keys are metric names and
            values are sequences of bucket boundaries (e.g.
            ``{"workflow_task_schedule_to_start_latency": [0.01, 0.05, 0.1, 0.5, 1.0, 5.0]}``).
    """

    bind_address: str
    counters_total_suffix: bool = False
    unit_suffix: bool = False
    durations_as_seconds: bool = False
    histogram_bucket_overrides: Mapping[str, Sequence[float]] | None = None

    def _to_bridge_config(self) -> temporalio.bridge.runtime.PrometheusConfig:
        return temporalio.bridge.runtime.PrometheusConfig(
            bind_address=self.bind_address,
            counters_total_suffix=self.counters_total_suffix,
            unit_suffix=self.unit_suffix,
            durations_as_seconds=self.durations_as_seconds,
            histogram_bucket_overrides=self.histogram_bucket_overrides,
        )


class MetricBufferDurationFormat(Enum):
    """How durations are represented for metrics buffers."""

    MILLISECONDS = 1
    """Durations are millisecond integers."""

    SECONDS = 2
    """Durations are second floats."""


class MetricBuffer:
    """A buffer that can be set on :py:class:`TelemetryConfig` to record
    metrics instead of ignoring/exporting them.

    .. warning::
        It is important that the buffer size is set to a high number and that
        :py:meth:`retrieve_updates` is called regularly to drain the buffer. If
        the buffer is full, metric updates will be dropped and an error will be
        logged.
    """

    def __init__(
        self,
        buffer_size: int,
        duration_format: MetricBufferDurationFormat = MetricBufferDurationFormat.MILLISECONDS,
    ) -> None:
        """Create a buffer with the given size.

        .. warning::
            It is important that the buffer size is set to a high number and is
            drained regularly. See :py:class:`MetricBuffer` warning.

        Args:
            buffer_size: Size of the buffer. Set this to a large value. A value
                in the tens of thousands or higher is plenty reasonable.
            duration_format: Which duration format to use.
        """
        self._buffer_size = buffer_size
        self._runtime: Runtime | None = None
        self._durations_as_seconds = (
            duration_format == MetricBufferDurationFormat.SECONDS
        )

    def retrieve_updates(self) -> Sequence[BufferedMetricUpdate]:
        """Drain the buffer and return all metric updates.

        .. warning::
            It is important that this is called regularly. See
            :py:class:`MetricBuffer` warning.

        Returns:
            A sequence of metric updates.
        """
        if not self._runtime:
            raise RuntimeError("Attempting to retrieve updates before runtime created")
        return self._runtime._core_runtime.retrieve_buffered_metrics(
            self._durations_as_seconds
        )


@dataclass(frozen=True)
class TelemetryConfig:
    """Configuration for Core telemetry."""

    logging: LoggingConfig | None = LoggingConfig.default
    """Logging configuration."""

    metrics: OpenTelemetryConfig | PrometheusConfig | MetricBuffer | None = None
    """Metrics configuration or buffer."""

    global_tags: Mapping[str, str] = field(default_factory=dict)
    """OTel resource tags to be applied to all metrics."""

    attach_service_name: bool = True
    """Whether to put the service_name on every metric."""

    metric_prefix: str | None = None
    """Prefix to put on every Temporal metric. If unset, defaults to
    ``temporal_``."""

    def _to_bridge_config(self) -> temporalio.bridge.runtime.TelemetryConfig:
        return temporalio.bridge.runtime.TelemetryConfig(
            logging=None if not self.logging else self.logging._to_bridge_config(),
            metrics=None
            if not self.metrics
            else temporalio.bridge.runtime.MetricsConfig(
                opentelemetry=None
                if not isinstance(self.metrics, OpenTelemetryConfig)
                else self.metrics._to_bridge_config(),
                prometheus=None
                if not isinstance(self.metrics, PrometheusConfig)
                else self.metrics._to_bridge_config(),
                buffered_with_size=0
                if not isinstance(self.metrics, MetricBuffer)
                else self.metrics._buffer_size,
                attach_service_name=self.attach_service_name,
                global_tags=self.global_tags or None,
                metric_prefix=self.metric_prefix,
            ),
        )


BufferedMetricKind = NewType("BufferedMetricKind", int)
"""Representation of a buffered metric kind."""

BUFFERED_METRIC_KIND_COUNTER = BufferedMetricKind(0)
"""Buffered metric is a counter which means values are deltas."""

BUFFERED_METRIC_KIND_GAUGE = BufferedMetricKind(1)
"""Buffered metric is a gauge."""

BUFFERED_METRIC_KIND_HISTOGRAM = BufferedMetricKind(2)
"""Buffered metric is a histogram."""


# WARNING: This must match Rust metric::BufferedMetric
class BufferedMetric(Protocol):
    """A metric for a buffered update.

    The same metric for the same name and runtime is guaranteed to be the exact
    same object for performance reasons. This means py:func:`id` will be the
    same for the same metric across updates.
    """

    @property
    def name(self) -> str:
        """Get the name of the metric."""
        ...

    @property
    def description(self) -> str | None:
        """Get the description of the metric if any."""
        ...

    @property
    def unit(self) -> str | None:
        """Get the unit of the metric if any."""
        ...

    @property
    def kind(self) -> BufferedMetricKind:
        """Get the metric kind.

        This is one of :py:const:`BUFFERED_METRIC_KIND_COUNTER`,
        :py:const:`BUFFERED_METRIC_KIND_GAUGE`, or
        :py:const:`BUFFERED_METRIC_KIND_HISTOGRAM`.
        """
        ...


# WARNING: This must match Rust metric::BufferedMetricUpdate
class BufferedMetricUpdate(Protocol):
    """A single metric value update."""

    @property
    def metric(self) -> BufferedMetric:
        """Metric being updated.

        For performance reasons, this is the same object across updates for the
        same metric. This means py:func:`id` will be the same for the same
        metric across updates.
        """
        ...

    @property
    def value(self) -> int | float:
        """Value for the update.

        For counters this is a delta, for gauges and histograms this is just the
        value.
        """
        ...

    @property
    def attributes(self) -> temporalio.common.MetricAttributes:
        """Attributes for the update.

        For performance reasons, this is the same object across updates for the
        same attribute set. This means py:func:`id` will be the same for the
        same attribute set across updates. Note this is for same "attribute set"
        as created by the metric creator, but different attribute sets may have
        the same values.

        Do not mutate this.
        """
        ...


class _MetricMeter(temporalio.common.MetricMeter):
    def __init__(
        self,
        core_meter: temporalio.bridge.metric.MetricMeter,
        core_attrs: temporalio.bridge.metric.MetricAttributes,
    ) -> None:
        self._core_meter = core_meter
        self._core_attrs = core_attrs

    def create_counter(
        self, name: str, description: str | None = None, unit: str | None = None
    ) -> temporalio.common.MetricCounter:
        return _MetricCounter(
            name,
            description,
            unit,
            temporalio.bridge.metric.MetricCounter(
                self._core_meter, name, description, unit
            ),
            self._core_attrs,
        )

    def create_histogram(
        self, name: str, description: str | None = None, unit: str | None = None
    ) -> temporalio.common.MetricHistogram:
        return _MetricHistogram(
            name,
            description,
            unit,
            temporalio.bridge.metric.MetricHistogram(
                self._core_meter, name, description, unit
            ),
            self._core_attrs,
        )

    def create_histogram_float(
        self, name: str, description: str | None = None, unit: str | None = None
    ) -> temporalio.common.MetricHistogramFloat:
        return _MetricHistogramFloat(
            name,
            description,
            unit,
            temporalio.bridge.metric.MetricHistogramFloat(
                self._core_meter, name, description, unit
            ),
            self._core_attrs,
        )

    def create_histogram_timedelta(
        self, name: str, description: str | None = None, unit: str | None = None
    ) -> temporalio.common.MetricHistogramTimedelta:
        return _MetricHistogramTimedelta(
            name,
            description,
            unit,
            temporalio.bridge.metric.MetricHistogramDuration(
                self._core_meter, name, description, unit
            ),
            self._core_attrs,
        )

    def create_gauge(
        self, name: str, description: str | None = None, unit: str | None = None
    ) -> temporalio.common.MetricGauge:
        return _MetricGauge(
            name,
            description,
            unit,
            temporalio.bridge.metric.MetricGauge(
                self._core_meter, name, description, unit
            ),
            self._core_attrs,
        )

    def create_gauge_float(
        self, name: str, description: str | None = None, unit: str | None = None
    ) -> temporalio.common.MetricGaugeFloat:
        return _MetricGaugeFloat(
            name,
            description,
            unit,
            temporalio.bridge.metric.MetricGaugeFloat(
                self._core_meter, name, description, unit
            ),
            self._core_attrs,
        )

    def with_additional_attributes(
        self, additional_attributes: temporalio.common.MetricAttributes
    ) -> temporalio.common.MetricMeter:
        return _MetricMeter(
            self._core_meter,
            self._core_attrs.with_additional_attributes(additional_attributes),
        )


_CoreMetricType = TypeVar("_CoreMetricType")


class _MetricCommon(temporalio.common.MetricCommon, Generic[_CoreMetricType]):
    def __init__(
        self,
        name: str,
        description: str | None,
        unit: str | None,
        core_metric: _CoreMetricType,
        core_attrs: temporalio.bridge.metric.MetricAttributes,
    ) -> None:
        self._name = name
        self._description = description
        self._unit = unit
        self._core_metric = core_metric
        self._core_attrs = core_attrs

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str | None:
        return self._description

    @property
    def unit(self) -> str | None:
        return self._unit

    def with_additional_attributes(
        self, additional_attributes: temporalio.common.MetricAttributes
    ) -> Self:
        return self.__class__(
            self._name,
            self._description,
            self._unit,
            self._core_metric,
            self._core_attrs.with_additional_attributes(additional_attributes),
        )


class _MetricCounter(
    temporalio.common.MetricCounter,
    _MetricCommon[temporalio.bridge.metric.MetricCounter],
):
    def add(
        self,
        value: int,
        additional_attributes: temporalio.common.MetricAttributes | None = None,
    ) -> None:
        if value < 0:
            raise ValueError("Metric value cannot be negative")
        core_attrs = self._core_attrs
        if additional_attributes:
            core_attrs = core_attrs.with_additional_attributes(additional_attributes)
        self._core_metric.add(value, core_attrs)


class _MetricHistogram(
    temporalio.common.MetricHistogram,
    _MetricCommon[temporalio.bridge.metric.MetricHistogram],
):
    def record(
        self,
        value: int,
        additional_attributes: temporalio.common.MetricAttributes | None = None,
    ) -> None:
        if value < 0:
            raise ValueError("Metric value cannot be negative")
        core_attrs = self._core_attrs
        if additional_attributes:
            core_attrs = core_attrs.with_additional_attributes(additional_attributes)
        self._core_metric.record(value, core_attrs)


class _MetricHistogramFloat(
    temporalio.common.MetricHistogramFloat,
    _MetricCommon[temporalio.bridge.metric.MetricHistogramFloat],
):
    def record(
        self,
        value: float,
        additional_attributes: temporalio.common.MetricAttributes | None = None,
    ) -> None:
        if value < 0:
            raise ValueError("Metric value cannot be negative")
        core_attrs = self._core_attrs
        if additional_attributes:
            core_attrs = core_attrs.with_additional_attributes(additional_attributes)
        self._core_metric.record(value, core_attrs)


class _MetricHistogramTimedelta(
    temporalio.common.MetricHistogramTimedelta,
    _MetricCommon[temporalio.bridge.metric.MetricHistogramDuration],
):
    def record(
        self,
        value: timedelta,
        additional_attributes: temporalio.common.MetricAttributes | None = None,
    ) -> None:
        if value.days < 0:
            raise ValueError("Metric value cannot be negative")
        core_attrs = self._core_attrs
        if additional_attributes:
            core_attrs = core_attrs.with_additional_attributes(additional_attributes)
        self._core_metric.record(
            (value.days * 86400 * 1000)
            + (value.seconds * 1000)
            + (value.microseconds // 1000),
            core_attrs,
        )


class _MetricGauge(
    temporalio.common.MetricGauge, _MetricCommon[temporalio.bridge.metric.MetricGauge]
):
    def set(
        self,
        value: int,
        additional_attributes: temporalio.common.MetricAttributes | None = None,
    ) -> None:
        if value < 0:
            raise ValueError("Metric value cannot be negative")
        core_attrs = self._core_attrs
        if additional_attributes:
            core_attrs = core_attrs.with_additional_attributes(additional_attributes)
        self._core_metric.set(value, core_attrs)


class _MetricGaugeFloat(
    temporalio.common.MetricGaugeFloat,
    _MetricCommon[temporalio.bridge.metric.MetricGaugeFloat],
):
    def set(
        self,
        value: float,
        additional_attributes: temporalio.common.MetricAttributes | None = None,
    ) -> None:
        if value < 0:
            raise ValueError("Metric value cannot be negative")
        core_attrs = self._core_attrs
        if additional_attributes:
            core_attrs = core_attrs.with_additional_attributes(additional_attributes)
        self._core_metric.set(value, core_attrs)
