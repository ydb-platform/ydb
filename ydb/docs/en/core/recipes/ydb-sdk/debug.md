# Troubleshooting

When troubleshooting issues with {{ ydb-short-name }}, diagnostics tools such as logging, metrics, and distributed tracing are helpful. We strongly recommend that you enable them in advance, before any problems occur, to see the full picture of the system's state before, during, and after a failure when investigating an incident.

This section contains code recipes for enabling diagnostics tools in different {{ ydb-short-name }} SDKs.

Table of contents:

- [Enable logging](debug-logs.md)
- [Enable metrics in Prometheus](debug-prometheus.md)
- [Enable tracing in OpenTelemetry](debug-otel.md)
- [Enable tracing in Jaeger](debug-jaeger.md)
