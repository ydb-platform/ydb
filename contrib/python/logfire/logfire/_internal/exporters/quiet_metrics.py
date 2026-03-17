from typing import Any

import requests
from opentelemetry.sdk.metrics.export import MetricExportResult, MetricsData

from .wrapper import WrapperMetricExporter


class QuietMetricExporter(WrapperMetricExporter):
    """A MetricExporter that catches request exceptions to prevent OTEL from logging a huge traceback."""

    def export(self, metrics_data: MetricsData, timeout_millis: float = 10_000, **kwargs: Any) -> MetricExportResult:
        try:
            return super().export(metrics_data, timeout_millis, **kwargs)
        except requests.exceptions.RequestException:
            # Rely on OTLPExporterHttpSession to log this kind of error periodically.
            return MetricExportResult.FAILURE
