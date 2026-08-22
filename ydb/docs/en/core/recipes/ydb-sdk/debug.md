# Diagnosing problems

When diagnosing problems related to {{ ydb-short-name }}, diagnostic tools such as logging, metrics, and OpenTracing/Jaeger traces help. We strongly recommend enabling diagnostic tools in advance, before problems occur. At the very least, during an investigation you can see changes in the picture before, during, and after the problems. This greatly speeds up incident investigation and our assistance.

This section contains code recipes for enabling diagnostic tools in different {{ ydb-short-name }} SDKs.

Contents:

- [Enable logging](debug-logs.md)
- [Enable metrics in Prometheus](debug-prometheus.md)
- [Enable tracing in Jaeger](debug-jaeger.md)
- [Enable tracing in OpenTelemetry](debug-otel.md)
