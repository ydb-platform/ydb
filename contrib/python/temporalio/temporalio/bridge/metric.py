"""Metrics using SDK Core. (unstable)

Nothing in this module should be considered stable. The API may change.
"""

from __future__ import annotations

from typing import Mapping, Optional, Union

import temporalio.bridge.runtime
import temporalio.bridge.temporal_sdk_bridge


class MetricMeter:
    """Metric meter using SDK Core."""

    @staticmethod
    def create(runtime: temporalio.bridge.runtime.Runtime) -> Optional[MetricMeter]:
        """Create optional metric meter."""
        ref = temporalio.bridge.temporal_sdk_bridge.new_metric_meter(runtime._ref)
        if not ref:
            return None
        return MetricMeter(ref)

    def __init__(
        self, ref: temporalio.bridge.temporal_sdk_bridge.MetricMeterRef
    ) -> None:
        """Initialize metric meter."""
        self._ref = ref
        self._default_attributes = MetricAttributes(self, ref.default_attributes)

    @property
    def default_attributes(self) -> MetricAttributes:
        """Default attributes for the metric meter."""
        return self._default_attributes


class MetricCounter:
    """Metric counter using SDK Core."""

    def __init__(
        self,
        meter: MetricMeter,
        name: str,
        description: Optional[str],
        unit: Optional[str],
    ) -> None:
        """Initialize counter metric."""
        self._ref = meter._ref.new_counter(name, description, unit)

    def add(self, value: int, attrs: MetricAttributes) -> None:
        """Add value to counter."""
        if value < 0:
            raise ValueError("Metric value must be non-negative value")
        self._ref.add(value, attrs._ref)


class MetricHistogram:
    """Metric histogram using SDK Core."""

    def __init__(
        self,
        meter: MetricMeter,
        name: str,
        description: Optional[str],
        unit: Optional[str],
    ) -> None:
        """Initialize histogram."""
        self._ref = meter._ref.new_histogram(name, description, unit)

    def record(self, value: int, attrs: MetricAttributes) -> None:
        """Record value on histogram."""
        if value < 0:
            raise ValueError("Metric value must be non-negative value")
        self._ref.record(value, attrs._ref)


class MetricHistogramFloat:
    """Metric histogram using SDK Core."""

    def __init__(
        self,
        meter: MetricMeter,
        name: str,
        description: Optional[str],
        unit: Optional[str],
    ) -> None:
        """Initialize histogram."""
        self._ref = meter._ref.new_histogram_float(name, description, unit)

    def record(self, value: float, attrs: MetricAttributes) -> None:
        """Record value on histogram."""
        if value < 0:
            raise ValueError("Metric value must be non-negative value")
        self._ref.record(value, attrs._ref)


class MetricHistogramDuration:
    """Metric histogram using SDK Core."""

    def __init__(
        self,
        meter: MetricMeter,
        name: str,
        description: Optional[str],
        unit: Optional[str],
    ) -> None:
        """Initialize histogram."""
        self._ref = meter._ref.new_histogram_duration(name, description, unit)

    def record(self, value_ms: int, attrs: MetricAttributes) -> None:
        """Record value on histogram."""
        if value_ms < 0:
            raise ValueError("Metric value must be non-negative value")
        self._ref.record(value_ms, attrs._ref)


class MetricGauge:
    """Metric gauge using SDK Core."""

    def __init__(
        self,
        meter: MetricMeter,
        name: str,
        description: Optional[str],
        unit: Optional[str],
    ) -> None:
        """Initialize gauge."""
        self._ref = meter._ref.new_gauge(name, description, unit)

    def set(self, value: int, attrs: MetricAttributes) -> None:
        """Set value on gauge."""
        if value < 0:
            raise ValueError("Metric value must be non-negative value")
        self._ref.set(value, attrs._ref)


class MetricGaugeFloat:
    """Metric gauge using SDK Core."""

    def __init__(
        self,
        meter: MetricMeter,
        name: str,
        description: Optional[str],
        unit: Optional[str],
    ) -> None:
        """Initialize gauge."""
        self._ref = meter._ref.new_gauge_float(name, description, unit)

    def set(self, value: float, attrs: MetricAttributes) -> None:
        """Set value on gauge."""
        if value < 0:
            raise ValueError("Metric value must be non-negative value")
        self._ref.set(value, attrs._ref)


class MetricAttributes:
    """Metric attributes using SDK Core."""

    def __init__(
        self,
        meter: MetricMeter,
        ref: temporalio.bridge.temporal_sdk_bridge.MetricAttributesRef,
    ) -> None:
        """Initialize attributes."""
        self._meter = meter
        self._ref = ref

    def with_additional_attributes(
        self, new_attrs: Mapping[str, Union[str, int, float, bool]]
    ) -> MetricAttributes:
        """Create new attributes with new attributes appended."""
        return MetricAttributes(
            self._meter,
            self._ref.with_additional_attributes(self._meter._ref, new_attrs),
        )
