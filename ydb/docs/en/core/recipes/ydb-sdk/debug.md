# Problem diagnostics

When diagnosing problems related to {{ ydb-short-name }}, diagnostic tools help: logging, metrics, and distributed tracing. It is recommended to enable them in advance, before problems occur, so that when investigating an incident, you can see the full picture of the system state before, during, and after the failure.

This section contains code recipes for enabling diagnostic tools in different {{ ydb-short-name }} SDKs.

Contents:

- [Enable logging](debug-logs.md)
- [Connect metrics to Prometheus](debug-prometheus.md)
- [Tracing with OpenTelemetry](debug-otel-tracing.md)
- [Metrics with OpenTelemetry](debug-otel-metrics.md)
- [Connect tracing to Jaeger](debug-jaeger.md)
