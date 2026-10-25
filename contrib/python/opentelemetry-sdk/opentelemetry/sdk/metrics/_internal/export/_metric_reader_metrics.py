# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from collections import Counter
from typing import Protocol

from opentelemetry.metrics import MeterProvider
from opentelemetry.semconv._incubating.attributes.otel_attributes import (
    OTEL_COMPONENT_NAME,
    OTEL_COMPONENT_TYPE,
)
from opentelemetry.semconv._incubating.metrics.otel_metrics import (
    create_otel_sdk_metric_reader_collection_duration,
)

_component_counter = Counter()


class MetricReaderMetricsT(Protocol):
    def record_collection(self, duration: float) -> None: ...


class NoOpMetricReaderMetrics:
    def record_collection(self, duration: float) -> None:
        pass


class MetricReaderMetrics:
    def __init__(
        self, component_type: str, meter_provider: MeterProvider
    ) -> None:
        meter = meter_provider.get_meter("opentelemetry-sdk")

        count = _component_counter[component_type]
        _component_counter[component_type] = count + 1

        self._standard_attrs = {
            OTEL_COMPONENT_TYPE: component_type,
            OTEL_COMPONENT_NAME: f"{component_type}/{count}",
        }

        self._collection_duration = (
            create_otel_sdk_metric_reader_collection_duration(meter)
        )

    def record_collection(self, duration: float) -> None:
        self._collection_duration.record(duration, self._standard_attrs)


def create_metric_reader_metrics(
    component_type: str,
    meter_provider: MeterProvider,
    enabled: bool,
) -> MetricReaderMetricsT:
    if not enabled:
        return NoOpMetricReaderMetrics()

    return MetricReaderMetrics(component_type, meter_provider)
