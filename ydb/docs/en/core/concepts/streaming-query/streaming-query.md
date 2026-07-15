# Streaming queries

A streaming query is a type of query designed for continuous processing of an unbounded data stream ( [stream processing](https://en.wikipedia.org/wiki/Stream_processing)). Unlike regular queries, a streaming query does not terminate after receiving a result but runs continuously, processing data as it arrives.

Stream processing is a widely used approach implemented in systems such as [Apache Flink](https://flink.apache.org/), [Apache Kafka Streams](https://kafka.apache.org/documentation/streams/), and [Amazon Kinesis Data Analytics](https://aws.amazon.com/kinesis/data-analytics/). Typical use cases include monitoring and alerting, metric aggregation over time windows, real-time event transformation and filtering, enriching events with reference data, and pattern detection in event sequences.

{{ ydb-short-name }} implements stream processing as part of a unified data platform. In addition to typical streaming query use cases, integration into the {{ydb-short-name}} unified platform allows you to read data from [topics](../datamodel/topic.md) of {{ydb-short-name}}, process change streams from [row tables](../datamodel/table.md#row-oriented-tables) via [CDC](../cdc.md), and write processing results to output topics or directly to tables. This enables building data processing pipelines within {{ydb-short-name}} with minimal latency.

## Differences from regular queries {#differences}

Regular queries work with data already stored in tables. A query executes, returns a result, and terminates. A streaming query is created and runs indefinitely until explicitly canceled by the user. Data continuously arrives in a topic, "flows through" the query, and is written to a sink — another topic or table.

| Characteristic | Regular queries | Streaming queries |
| --- | --- | --- |
| Data | Finite set in tables | Infinite stream of events |
| Lifetime | Terminates after processing | Runs continuously |
| Result | Available after completion | Updated as data arrives |
| Recovery on failures | Manual restart | Automatic recovery from [checkpoint](../../dev/streaming-query/checkpoints.md) |

## Data sources and sinks {#data-flow}

Streaming queries read data from [topics](../datamodel/topic.md) of {{ ydb-short-name }} and write results to [topics](../datamodel/topic.md) or [tables](../datamodel/table.md) of {{ ydb-short-name }}. Data can be written to a topic from external systems (for example, an application sends events via the SDK), but the streaming query itself works only with {{ ydb-short-name }} entities. Direct reading from external systems, such as Apache Kafka, or writing to tables in other databases, such as PostgreSQL, is not supported.

### Sources {#sources}

**Topics** are the primary source of streaming data. The query reads messages from one or more [topics](../datamodel/topic.md) and processes them as they arrive. Data can be written to a topic by:

* **External applications** — via the [{{ ydb-short-name }} SDK](../../reference/ydb-sdk/index.md) or [Kafka API](../../reference/kafka-api/index.md). For example, a service sends telemetry events or logs to a {{ ydb-short-name }} topic, and the streaming query processes them.
* **CDC (Change Data Capture)** — change streams from tables implemented through built-in [topics](../datamodel/topic.md). They allow you to react to inserts, updates, and deletes of records in real time. For more information, see [{#T}](../cdc.md).

### Sinks {#sinks}

**Topics** — for passing results to other systems or subsequent processing stages.

**Tables** — for materializing results. Data is saved via [UPSERT](../../yql/reference/syntax/upsert_into.md) and is available for regular SQL queries.

## Guarantees {#guarantees}

Under normal operation, streaming queries provide [at-least-once](https://en.wikipedia.org/wiki/Reliable_messaging#At-least-once_delivery) data delivery guarantees — on failures, the query automatically recovers from a [checkpoint](../../dev/streaming-query/checkpoints.md) and resumes reading from the saved offsets.

However, it is important to consider the limitations of the current implementation:

- **Offset reset on query recreation:** changing the query text via [DROP](../../yql/reference/syntax/drop-streaming-query.md) + [CREATE](../../yql/reference/syntax/create-streaming-query.md) deletes the checkpoint. The new query starts reading from the end of the topic, skipping events that arrived between the deletion of the old version and the start of the new one.
- **Incomplete aggregates due to lack of watermarks:** due to the absence of a [watermarks](https://en.wikipedia.org/wiki/Watermark_(data_synchronization)) mechanism, time windows are closed based on system time (wall-clock). Events that arrive late (for example, due to slow partitions or network lag) are not included in the aggregates of already closed windows.

A detailed description of guarantees, anomalies, and ways to minimize them is in the [{#T}](../../dev/streaming-query/guarantees.md) section.

{% note info %}

We are constantly working on developing streaming processing mechanisms. The guarantees provided will be improved in future versions.

{% endnote %}

## Limitations {#limitations}

{% note warning %}

- The query must contain at least one read from a topic, as streaming processing requires a continuous input data stream.
- `JOIN` of two streams is not supported (temporary architectural limitation).

{% endnote %}

The following are also not supported in the current version:

- The [important reader](../datamodel/topic.md#important-consumer) flag for consumers used by streaming queries.
- [Auto-partitioning](../datamodel/topic.md#autopartitioning) (split/merge of partitions) of topics used by streaming queries. When the number of partitions in a topic read by a running streaming query increases, new partitions will not be processed.
- Changing the text of a running query. To change a query, you need to recreate it — delete it using [DROP STREAMING QUERY](../../yql/reference/syntax/drop-streaming-query.md) and create it again using [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md).

## Management {#control}

Streaming queries are created, modified, and deleted using YQL commands:

- [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md) — creation;
- [ALTER STREAMING QUERY](../../yql/reference/syntax/alter-streaming-query.md) — modification and state management;
- [DROP STREAMING QUERY](../../yql/reference/syntax/drop-streaming-query.md) — deletion.

The state of queries is available in the system table [`.sys/streaming_queries`](../../dev/system-views.md#streaming).

## Query language {#syntax}

Streaming queries are written in [YQL](../../yql/reference/index.md) and support familiar SQL constructs: [SELECT](../../yql/reference/syntax/select/index.md), [WHERE](../../yql/reference/syntax/select/where.md), [GROUP BY](../../yql/reference/syntax/select/group-by.md), [JOIN](../../yql/reference/syntax/select/join.md). [GROUP BY HOP](../../yql/reference/syntax/select/group-by.md#group-by-hop) is used for working with time windows{% if feature_match_recogznize==true %}, and [MATCH_RECOGNIZE](../../yql/reference/syntax/select/match_recognize.md) is used for pattern matching{% endif %}.

## See also

- [{#T}](../../dev/streaming-query/guarantees.md) — guarantees, anomalies in window aggregation, and recommendations;
- [{#T}](../../recipes/streaming_queries/topics.md) — step-by-step guide;
- [{#T}](../../dev/streaming-query/streaming-query-formats.md) — data formats.
