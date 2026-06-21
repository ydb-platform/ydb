# Passing external trace-id to {{ ydb-short-name }}

## gRPC API

{{ ydb-short-name }} supports passing external trace-ids to build a complete operation trace. The trace-id is passed according to the [trace context specification](https://w3c.github.io/trace-context/#traceparent-header) — a string of the form `00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01` must be passed through the `traceparent` header of the gRPC request. It consists of 4 parts separated by the `-` character:

1. Version – at the time of writing, the specification defines only version 00.
2. Trace id – the identifier of the trace that the request is part of.
3. Parent id – the identifier of the parent span.
4. Flags – a set of recommendations for working with the data passed in the header.

Only version 00 is supported; flags are ignored. If the header does not conform to the specification, it is ignored. All traces obtained this way have [detail level](./setup.md#tracing-levels) 13 (all components are traced at level `Detailed`).

{% note warning %}

If the [`external_throttling`](./setup.md#external-throttling) section is present and the request flow exceeds the set limits, not all requests may be traced. If the [`external_throttling`](./setup.md#external-throttling) section is absent, the `traceparent` header is **ignored** and no external traces are continued.

{% endnote %}

## SDK support

Some SDKs support trace-id passing; you can find their list and usage examples on the [{#T}](../../ydb-sdk/observability/tracing/opentelemetry.md) page.
