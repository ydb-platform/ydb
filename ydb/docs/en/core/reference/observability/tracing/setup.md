# Tracing in {{ ydb-short-name }}

{% note info %}

Tracing is described in detail on the [opentelemetry](https://opentelemetry.io/) website in the [observability primer](https://opentelemetry.io/docs/concepts/observability-primer/) section.

{% endnote %}

Tracing is a tool that allows you to see in detail the path a request takes through a distributed system. The path of a single request (trace) is described by collection of spans. A span represents a segment of time, usually associated with the time taken to perform a specific operation (e.g., writing information to disk, executing transaction). Spans form a tree, often a span's subtree is a detailed representation of it, but this is not always the case.

![Example of a trace](_images/spans.png "Example of a trace")

To aggregate disparate spans into traces, they are sent to a collector. This is a service that aggregates and stores spans for later analysis of traces. {{ ydb-short-name }} does not include this service; an administrator must set it up independently. [Jaeger](https://www.jaegertracing.io/) is commonly used as a collector.

## Minimal configuration

To enable tracing in {{ ydb-short-name }}, add the following section to your [configuration](../../../deploy/configuration/config.md):

```yaml
tracing_config:
  backend:
    opentelemetry:
      collector_url: grpc://example.com:4317
      service_name: ydb
  external_throttling:
    - max_traces_per_minute: 10
```

In this configuration, the `collector_url` field specifies the URL of an [OTLP-compatible](https://opentelemetry.io/docs/specs/otlp/) span collector. More details on the `backend` section can be found in the [corresponding section](./setup.md#backend).

With this setup, no requests are sampled, and no more than 10 requests per minute with an [external trace-id](./external-traces.md) are traced by each cluster node.

## Section descriptions

### Backend {#backend}

#### Section example

```yaml
tracing_config:
  # ...
  backend:
    opentelemetry:
      collector_url: grpc://example.com:4317
      service_name: ydb
```

#### Description

This section describes the span collector. Currently, the only supported option is `opentelemetry`. Span transmission from a cluster node to the collector is performed using the push model. The collector must be compatible with [OTLP](https://opentelemetry.io/docs/specs/otlp/).

In the opentelemetry section:
* `collector_url` — URL of the span collector. The scheme can be either `grpc://` for an insecure connection or `grpcs://` for a TLS connection.
* `service_name` — the name of the service with which all spans will be tagged.

Both parameters are mandatory.

### Uploader {#uploader}

#### Section example

```yaml
tracing_config:
  # ...
  uploader:
    max_exported_spans_per_second: 30
    max_spans_in_batch: 100
    max_bytes_in_batch: 10485760 # 10 MiB
    max_export_requests_inflight: 3
    max_batch_accumulation_milliseconds: 5000
    span_export_timeout_seconds: 120
```

#### Description

The uploader is a component of a cluster node responsible for sending spans to the collector. To avoid overloading the span collector, the uploader will not send more than `max_exported_spans_per_second` spans per second on average.

To optimize performance and reduce the number of requests, the uploader sends spans in batches. Each batch contains no more than `max_spans_in_batch` spans with a total serialized size of no more than `max_bytes_in_batch` bytes. Each batch accumulates for no more than `max_batch_accumulation_milliseconds` milliseconds. Batches can be sent in parallel, and the maximum number of concurrently sent batches is controlled by the `max_export_requests_inflight` parameter. If more than `span_export_timeout_seconds` seconds have passed since a span was received by the uploader, the uploader may discard it in favor of sending newer spans.

Default values:
* `max_exported_spans_per_second = inf` (no limits)
* `max_spans_in_batch = 150`
* `max_bytes_in_batch = 20000000`
* `max_batch_accumulation_milliseconds = 1000`
* `span_export_timeout_seconds = inf` (no spans are discarded by the uploader)
* `max_export_requests_inflight = 1`

The `uploader` section is optional; if absent, the default value for each parameter is used.

{% note info %}

The uploader is a local component of the node, so the described limits apply to each node separately, not to the entire cluster as a whole.

{% endnote %}

### External throttling {#external-throttling}

#### Section example

```yaml
tracing_config:
  # ...
  external_throttling:
    - scope:
        database: /Root/db1
      max_traces_per_minute: 60
      max_traces_burst: 3
```

#### Description

{{ ydb-short-name }} supports the transmission of external trace-ids for constructing a complete trace of a request. The method of transmitting an external trace-id is described on the page [{#T}](./external-traces.md). To avoid overloading the collector, {{ ydb-short-name }} has a mechanism to limit the number of externally traced requests. The restrictions are described in this section and consist of a sequence of rules, each rule containing:

* `scope` – a set of selectors for filtering the request.
* `max_traces_per_minute` – the highest average number of requests per minute to be traced by this rule. Should be a positive integer.
* `max_traces_burst` – the maximum burst of externally traced requests. Should be a non-negative integer.

The only mandatory parameter is `max_traces_per_minute`.

A detailed description of these options is provided in the section [{#T}](./setup.md#semantics)

The `external_throttling` section is optional; if absent, all trace-ids in requests are **ignored** (no external traces are continued).

This section can be modified without restarting the node using the [dynamic configuration](../../../maintenance/manual/dynamic-config.md) mechanism.

### Sampling

#### Section example

```yaml
tracing_config:
  # ...
  sampling:
    - fraction: 0.01
      level: 10
      max_traces_per_minute: 5
      max_traces_burst: 2
    - scope:
        request_types:
          - KeyValue.ExecuteTransaction
          - KeyValue.Read
      fraction: 0.1
      level: 15
      max_traces_per_minute: 5
      max_traces_burst: 2
```

#### Description

Diagnosing system issues can benefit from examining a sample trace of a request, whether or not users are actively tracing their requests. For this purpose, {{ ydb-short-name }} has a request sampling mechanism. For a sampled request, a random trace-id is generated. This section controls the sampling of requests with configuration format similar to [`external_throttling`](./setup.md#external-throttling), each rule has two additional fields:

* `fraction` – the proportion of requests sampled according to this rule. Should be a rational number between 0 and 1 inclusive.
* `level` — verbosity level of a trace. Should be an integer between 0 and 15 inclusive. This parameter is described in more detail in the section [{#T}](./setup.md#tracing-levels)

Both fields are mandatory.

The `sampling` section is optional; if absent, no requests will be sampled.

This section can be modified without restarting the node using the [dynamic configuration](../../../maintenance/manual/dynamic-config.md) mechanism.

## Rule semantics {#semantics}

### Selectors

Each rule includes an optional scope field with a set of selectors that determine which requests the rule applies to. Currently supported selectors are:

* `request_types`
    
    Accepts a list of request types. A request matches this selector if its type is in the list.
    
{% cut "Possible values" %}

- KeyValue.CreateVolume
- KeyValue.DropVolume
- KeyValue.AlterVolume
- KeyValue.DescribeVolume
- KeyValue.ListLocalPartitions
- KeyValue.AcquireLock
- KeyValue.ExecuteTransaction
- KeyValue.Read
- KeyValue.ReadRange
- KeyValue.ListRange
- KeyValue.GetStorageChannelStatus
- Table.CreateSession
- Table.KeepAlive
- Table.AlterTable
- Table.CreateTable
- Table.DropTable
- Table.DescribeTable
- Table.CopyTable
- Table.CopyTables
- Table.RenameTables
- Table.ExplainDataQuery
- Table.ExecuteSchemeQuery
- Table.BeginTransaction
- Table.DescribeTableOptions
- Table.DeleteSession
- Table.CommitTransaction
- Table.RollbackTransaction
- Table.PrepareDataQuery
- Table.ExecuteDataQuery
- Table.BulkUpsert
- Table.StreamExecuteScanQuery
- Table.StreamReadTable
- Table.ReadRows
- Query.ExecuteQuery
- Query.ExecuteScript
- Query.FetchScriptResults
- Query.CreateSession
- Query.DeleteSession
- Query.AttachSession
- Query.BeginTransaction
- Query.CommitTransaction
- Query.RollbackTransaction
- Discovery.WhoAmI
- Discovery.NodeRegistration
- Discovery.ListEndpoints

{% note info %}

Tracing is supported not only for the request types listed above. This list only includes request types supported by the `request_types` selector.

{% endnote %}

{% note warning %}

Please note that the QueryService API is [experimental](https://github.com/ydb-platform/ydb/blob/e3af273efaef7dfa21205278f17cd164e247820d/ydb/public/api/grpc/ydb_query_v1.proto#L9) and may change in the future.

{% endnote %}

{% endcut %}

* `database`

    Matches requests to a specified database.

A request matches the rule if it matches all selectors. `scope` can be absent, which is equivalent to an empty set of selectors, all requests will match this rule.

### Rate limiting

The parameters `max_traces_per_minute` and `max_traces_burst` are used to limit the number of requests. In the case of sampling, they limit the number of requests sampled by this rule. In the case of external throttling, they limit the number of external traces that are continued by the system.

A variation of the [leaky bucket](https://en.wikipedia.org/wiki/Leaky_bucket) with a bucket size of `max_traces_burst + 1` is used for limiting the amount of traced requests. For example, if `max_traces_per_minute = 60` and `max_traces_burst = 0`, then with a flow of 10000 requests per minute, one request will be traced every second. If `max_traces_burst = 20`, then with a similar flow of requests, the first 21 will be traced, and thereafter one request will be traced every second.

{% note warning %}

Limitations on the number of traced requests are local to the cluster node. For example, if there is a rule on each node of the cluster that specifies `max_traces_per_minute = 1`, no more than one request per minute will be traced **from each cluster node** according to this rule.

{% endnote %}

### Verbosity levels {#tracing-levels}

As with [logs](../../../maintenance/embedded_monitoring/logs.md), diagnosing most system problems does not require a maximally detailed trace, so in {{ ydb-short-name }} each span has its own verbosity level, described by an integer from 0 to 15 inclusive. Each rule in the sampling section must include the verbosity level of the generated trace (`level`), it will include spans with verbosity level less than or equal to `level` into the generated trace.

The [{{ ydb-short-name }} section](../../../concepts/_includes/index/how_it_works.md#ydb-architecture) section provides a division of the system into 5 layers:

| Layer | Components |
| ----- | ---------- |
| 1 | gRPC Proxies |
| 2 | Query Processor |
| 3 | Distributes Transactions |
| 4 | Tablet, System tablet |
| 5 | Distributed Storage |

There are 7 verbosity levels for each component:

| Verbosity level | Meaning |
| --------------- | ------- |
| `Off` | No tracing |
| `TopLevel` | The lowest verbosity, no more than 2 spans per request to the component are generated |
| `Basic` | Spans of the main operations of the component |
| `Detailed` | The highest verbosity, applicable for diagnosing problems in production |
| `Diagnostic` | Detailed debug information for developers |
| `Trace` | Very detailed debug information |

Below is a mapping from the trace verbosity level to the component verbosity levels:

| Trace verbosity level | gRPC Proxies | Query Processor | Distributed Transactions | Tablets | Distributed Storage |
| --------------------- | ------------ | --------------- | ------------------------ | ------- | ------------------- |
|  0 | `TopLevel` | `Off` | `Off` | `Off` | `Off` |
|  1 | `TopLevel` | **`TopLevel`** | `Off` | `Off` | `Off` |
|  2 | `TopLevel` | `TopLevel` | **`TopLevel`** | `Off` | `Off` |
|  3 | `TopLevel` | `TopLevel` | `TopLevel` | **`TopLevel`** | `Off` |
|  4 | `TopLevel` | `TopLevel` | `TopLevel` | `TopLevel` | **`TopLevel`** |
|  5 | **`Basic`** | `TopLevel` | `TopLevel` | `TopLevel` | `TopLevel` |
|  6 | `Basic` | **`Basic`** | `TopLevel` | `TopLevel` | `TopLevel` |
|  7 | `Basic` | `Basic` | **`Basic`** | `TopLevel` | `TopLevel` |
|  8 | `Basic` | `Basic` | `Basic` | **`Basic`** | `TopLevel` |
|  9 | `Basic` | `Basic` | `Basic` | `Basic` | **`Basic`** |
| 10 | **`Detailed`** | **`Detailed`** | `Basic` | `Basic` | `Basic` |
| 11 | `Detailed` | `Detailed` | **`Detailed`** | `Basic` | `Basic` |
| 12 | `Detailed` | `Detailed` | `Detailed` | **`Detailed`** | `Basic` |
| 13 | `Detailed` | `Detailed` | `Detailed` | `Detailed` | **`Detailed`** |
| 14 | **`Diagnostic`** | **`Diagnostic`** | **`Diagnostic`** | **`Diagnostic`** | **`Diagnostic`** |
| 15 | **`Trace`** | **`Trace`** | **`Trace`** | **`Trace`** | **`Trace`** |

### Rules

#### External Throttling

The semantics of each rule are such that it allocates a quota for the number of requests of a given category. For example, if the `external_throttling` section looks like this:

```yaml
tracing_config:
  external_throttling:
    - max_traces_per_minute: 60
    - scope:
        request_types:
          - KeyValue.ReadRange
      max_traces_per_minute: 20
```

With a sufficient flow of requests with an external trace-id, at least 60 requests per minute and at least 20 requests of type `KeyValue.ReadRange` will be traced per minute. In total, no more than 80 requests per minute will be traced.

The decision algorithm is as follows: for a request with an external trace-id, the set of rules under which the request falls is determined. The request consumes the quota from all rules where it is still available. A request is not traced only if no rule has any quota left.

#### Sampling

The semantics of the sampling rule are similar: with a sufficiently small flow of requests of a given category, at least `fraction` of the requests will be sampled with at least `level` verbosity level.

The decision algorithm is similar: for a request without an external trace-id (either due to its initial absence or due to a previous decision not to trace this request), the set of rules under which the request falls is determined. The request consumes the quota from all rules where it is still available and which randomly "decided" to sample it. If the request does not need to be sampled according to any rule (in all rules that randomly "decided" to sample the request, no quota is left), it is not sampled. Otherwise, the verbosity level is determined as the maximum among the rules into which the request fell.

For example, with the following sampling configuration:

```yaml
tracing_config:
  sampling:
    - scope:
        database: /Root/db1
      fraction: 0.5
      level: 5 
      max_traces_per_minute: 100
    - scope:
        database: /Root/db1
      fraction: 0.01
      level: 15
      max_traces_per_minute: 5
```

With a sufficiently small flow of requests to the `/Root/db1` database, will be sampled:

* 1% of requests with verbosity level of 15
* 49.5% of requests with verbosity level of 5

With a sufficiently large flow of requests to the `/Root/db1` database, will be sampled:

* 5 requests per minute with verbosity level of 15
* from 95 to 100 requests per minute with verbosity level of 5
