from .wrapper import WrapperMetricExporter as WrapperMetricExporter
from opentelemetry.sdk.metrics.export import MetricExportResult, MetricsData
from typing import Any

class QuietMetricExporter(WrapperMetricExporter):
    """A MetricExporter that catches request exceptions to prevent OTEL from logging a huge traceback."""
    def export(self, metrics_data: MetricsData, timeout_millis: float = 10000, **kwargs: Any) -> MetricExportResult: ...
