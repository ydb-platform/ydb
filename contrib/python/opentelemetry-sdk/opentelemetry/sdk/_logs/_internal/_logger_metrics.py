# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Protocol

from opentelemetry import metrics as metrics_api
from opentelemetry.semconv._incubating.metrics.otel_metrics import (
    create_otel_sdk_log_created,
)


class LoggerMetricsT(Protocol):
    def emit_log(self) -> None: ...


class NoOpLoggerMetrics:
    def emit_log(self) -> None:
        pass


class LoggerMetrics:
    def __init__(self, meter_provider: metrics_api.MeterProvider) -> None:
        meter = meter_provider.get_meter("opentelemetry-sdk")
        self._created_logs = create_otel_sdk_log_created(meter)

    def emit_log(self) -> None:
        self._created_logs.add(1)


def create_logger_metrics(
    meter_provider: metrics_api.MeterProvider,
    enabled: bool,
) -> LoggerMetricsT:
    if not enabled:
        return NoOpLoggerMetrics()

    return LoggerMetrics(meter_provider)
