from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from opentelemetry import context
from opentelemetry.sdk._logs import LogRecordProcessor, ReadableLogRecord, ReadWriteLogRecord
from opentelemetry.sdk._logs.export import LogRecordExporter, LogRecordExportResult
from opentelemetry.sdk.metrics.export import AggregationTemporality, MetricExporter, MetricExportResult, MetricsData
from opentelemetry.sdk.metrics.view import Aggregation
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

import logfire


class WrapperSpanExporter(SpanExporter):
    """A base class for SpanExporters that wrap another exporter."""

    def __init__(self, exporter: SpanExporter) -> None:
        self.wrapped_exporter = exporter

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        return self.wrapped_exporter.export(spans)

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self.wrapped_exporter.force_flush(timeout_millis)

    def shutdown(self) -> None:
        self.wrapped_exporter.shutdown()


class WrapperMetricExporter(MetricExporter):
    """A base class for MetricExporters that wrap another exporter."""

    def __init__(
        self,
        exporter: MetricExporter,
        preferred_temporality: dict[type, AggregationTemporality] | None = None,
        preferred_aggregation: dict[type, Aggregation] | None = None,
    ) -> None:
        super().__init__(preferred_temporality=preferred_temporality, preferred_aggregation=preferred_aggregation)  # type: ignore
        self.wrapped_exporter = exporter

    def export(self, metrics_data: MetricsData, timeout_millis: float = 10_000, **kwargs: Any) -> MetricExportResult:
        return self.wrapped_exporter.export(metrics_data, timeout_millis, **kwargs)  # type: ignore

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        return self.wrapped_exporter.force_flush(timeout_millis)

    def shutdown(self, timeout_millis: float = 30_000, **kwargs: Any) -> None:
        self.wrapped_exporter.shutdown(timeout_millis, **kwargs)  # type: ignore


@dataclass
class WrapperSpanProcessor(SpanProcessor):
    """A base class for SpanProcessors that wrap another processor."""

    processor: SpanProcessor

    def on_start(self, span: Span, parent_context: context.Context | None = None) -> None:
        self.processor.on_start(span, parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        self.processor.on_end(span)

    def shutdown(self) -> None:
        with logfire.suppress_instrumentation():
            self.processor.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        with logfire.suppress_instrumentation():
            return self.processor.force_flush(timeout_millis)


@dataclass
class WrapperLogExporter(LogRecordExporter):
    """A base class for LogExporters that wrap another exporter."""

    exporter: LogRecordExporter

    def export(self, batch: Sequence[ReadableLogRecord]) -> LogRecordExportResult:
        return self.exporter.export(batch)

    def shutdown(self):
        return self.exporter.shutdown()


@dataclass
class WrapperLogProcessor(LogRecordProcessor):
    """A base class for SpanProcessors that wrap another processor."""

    processor: LogRecordProcessor

    def on_emit(self, log_record: ReadWriteLogRecord) -> None:
        return self.processor.on_emit(log_record)

    def shutdown(self):
        return self.processor.shutdown()

    def force_flush(self, timeout_millis: int = 30000):
        return self.processor.force_flush(timeout_millis)
