# Observability in {{ ydb-short-name }} SDK

This section describes how to enable {{ ydb-short-name }} SDK diagnostic tools — logging, metrics, and distributed tracing. It is recommended to enable them in advance, before problems occur, so that when investigating an incident you can see the full picture of the system state before, during, and after the failure.

Logging:

- [{#T}](logging/logging.md)
- [{#T}](logging/opentelemetry.md)

Metrics:

- [{#T}](metrics/opentelemetry.md)
- [{#T}](metrics/prometheus.md)

Tracing:

- [{#T}](tracing/opentelemetry.md)
- [{#T}](tracing/jaeger.md)

Server-side observability {{ ydb-short-name }}, which is independent of the SDK, is described in the [{#T}](../../observability/index.md) section.
