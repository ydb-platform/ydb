# Providing an External Trace-ID to {{ ydb-short-name }}

## gRPC API

{{ ydb-short-name }} supports the transmission of external trace-ids for constructing a complete operation trace. The transmission of trace-id is carried out according to the [tracecontext specification](https://w3c.github.io/trace-context/#traceparent-header) — through the `traceparent` header of the gRPC request, a string of the form `00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01` should be transmitted. It consists of 4 parts, separated by the `-` symbol:

1. Version – at the time of writing the documentation, the specification defines only version 00.
1. Trace id – the identifier of the trace to which the request belongs.
1. Parent id – the identifier of the parent span.
1. Flags – a set of recommendations for working with the data transmitted in the header.

Only version 00 is supported, flags are ignored. If the header does not comply with the specification, it is ignored. All traces obtained in this way have a [verbosity level](./setup.md#tracing-levels) of 13 (all components are traced with the `Detailed` verbosity level).

{% note warning %}

In the presence of the [`external_throttling`](./setup.md#external-throttling) section and a flow of requests exceeding the set limits, not all requests may be traced. In the absence of the [`external_throttling`](./setup.md#external-throttling) section, the `traceparent` header is **ignored** and no external traces are continued.

{% endnote %}

## SDK Support

Some SDKs support the transmission of trace-id; you can find a list of these SDKs, as well as usage examples, on the page [{#T}](../../../reference/ydb-sdk/recipes/debug-jaeger.md).
