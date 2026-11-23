"""OpenTelemetry v2 integration for Temporal SDK.

This package provides OpenTelemetry tracing integration for Temporal workflows,
activities, and other operations. It includes automatic span creation and
propagation for distributed tracing. It also provides replay-safe wrappers for
the global OpenTelemetry tracer, meter, and logger providers.
"""

from temporalio.contrib.opentelemetry._interceptor import (
    TracingInterceptor,
    TracingWorkflowInboundInterceptor,
)
from temporalio.contrib.opentelemetry._logger_provider import (
    ReplaySafeLoggerProvider,
)
from temporalio.contrib.opentelemetry._meter_provider import (
    ReplaySafeMeterProvider,
)
from temporalio.contrib.opentelemetry._otel_interceptor import OpenTelemetryInterceptor
from temporalio.contrib.opentelemetry._plugin import OpenTelemetryPlugin
from temporalio.contrib.opentelemetry._tracer_provider import (
    ReplaySafeTracerProvider,
    create_tracer_provider,
)

__all__ = [
    "TracingInterceptor",
    "TracingWorkflowInboundInterceptor",
    "OpenTelemetryInterceptor",
    "OpenTelemetryPlugin",
    "ReplaySafeLoggerProvider",
    "ReplaySafeMeterProvider",
    "ReplaySafeTracerProvider",
    "create_tracer_provider",
]
