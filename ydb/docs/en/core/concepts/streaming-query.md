# Streaming queries

A **streaming query** is a query type designed for continuous processing of an unbounded data stream ([stream processing](https://en.wikipedia.org/wiki/Stream_processing)). Unlike regular queries, a streaming query does not finish after returning a result: it runs until it is explicitly stopped, processing data as it arrives.

Stream processing is a widely used approach implemented in systems such as [Apache Flink](https://flink.apache.org/), [Apache Kafka Streams](https://kafka.apache.org/documentation/streams/), and [Amazon Kinesis Data Analytics](https://aws.amazon.com/kinesis/data-analytics/). Typical use cases include monitoring and alerting, windowed metric aggregation, on-the-fly event transformation and filtering, enriching events with reference data, and pattern detection in event sequences.

{{ ydb-short-name }} implements stream processing as part of a unified data platform. Beyond common streaming scenarios, {{ ydb-short-name }} integration lets you read from [topics](datamodel/topic.md), process change streams from [row tables](datamodel/table.md#row-oriented-tables) via [CDC](cdc.md), and write results to output topics or directly to tables, enabling low-latency data pipelines inside {{ ydb-short-name }}.

## Differences from regular queries {#differences}

Regular queries work with data already stored in tables. A query runs, returns a result, and completes. A streaming query is created and runs indefinitely until the user cancels it. Data continuously flows into a topic, through the query, and to a sink—another topic or a table.

| Characteristic | Regular queries | Streaming queries |
|----------------|-----------------|-------------------|
| Data | A finite set in tables | An unbounded event stream |
| Lifetime | Completes after processing | Runs continuously |
| Result | Available when the query completes | Updates as data arrives |
| Recovery after failure | Manual restart | Automatic recovery from a [checkpoint](../dev/streaming-query/checkpoints.md) |

## Data sources and sinks {#data-flow}

Streaming queries read data from {{ ydb-short-name }} [topics](datamodel/topic.md) and write results to [topics](datamodel/topic.md) or [tables](datamodel/table.md). Data may reach a topic from external systems (for example, an application writes events via the SDK), but the streaming query only works with {{ ydb-short-name }} entities. Direct reads from external systems such as Apache Kafka, or writes to other databases such as PostgreSQL, are not supported.

### Sources {#sources}

**Topics** are the primary source of streaming data. The query reads messages from one or more [topics](datamodel/topic.md) as they arrive. Data can be written to a topic by:

* **External applications** — via the [{{ ydb-short-name }} SDK](../reference/ydb-sdk/index.md) or [Kafka API](../reference/kafka-api/index.md). For example, a service sends telemetry or logs to a {{ ydb-short-name }} topic, and a streaming query processes them.
* **CDC (Change Data Capture)** — change streams from tables implemented using built-in [topics](datamodel/topic.md). They let you react to inserts, updates, and deletes in near real time. See [{#T}](cdc.md).

### Sinks {#sinks}

**Topics** — for handing off results to other systems or downstream processing stages.

**Tables** — for materializing results. Data is written with [UPSERT](../yql/reference/syntax/upsert_into.md) and is available to regular SQL queries.

## Guarantees {#guarantees}

Under normal operation, streaming queries provide [at-least-once](https://en.wikipedia.org/wiki/Reliable_messaging#At-least-once_delivery) delivery: on failure, the query automatically recovers from a [checkpoint](../dev/streaming-query/checkpoints.md) and resumes reading from saved offsets.

Current implementation limitations:

- **Offset reset on query recreation:** changing the query text via [DROP](../yql/reference/syntax/drop-streaming-query.md) + [CREATE](../yql/reference/syntax/create-streaming-query.md) removes the checkpoint. The new query starts reading from the end of the topic, skipping events that arrived between dropping the old version and starting the new one.
- **Incomplete aggregates without watermarks:** without [watermarks](https://en.wikipedia.org/wiki/Watermark_(data_synchronization)), time windows close based on wall-clock time. Events that arrive late (for example, due to slow partitions or network lag) may not be included in aggregates for windows that have already closed.

For a detailed discussion of guarantees, anomalies, and mitigation, see [{#T}](../dev/streaming-query/guarantees.md).

{% note info %}

We are actively improving stream processing. Guarantees will be strengthened in future releases.

{% endnote %}

## Limitations {#limitations}

{% note warning %}

- The query must read from at least one topic, because streaming processing requires a continuous input stream.
- `JOIN` of two streams is not supported (a temporary architectural limitation).
- Enrichment from {{ ydb-short-name }} tables is not supported — streaming queries can only use [S3 external tables](query_execution/federated_query/s3/external_table.md) for enrichment (enrichment from {{ ydb-short-name }} tables is planned for release 26.1).
- Reading and writing local topics directly is not supported — use [external data sources](datamodel/external_data_source.md) pointing at the current database (this restriction is planned to be lifted in 26.1).

{% endnote %}

To work with local topics, use [external data sources](../concepts/datamodel/external_data_source.md):

- Create an [external data source](datamodel/external_data_source.md) pointing to the same or another {{ ydb-short-name }} database and access topics through it.

Also not supported in the current version:

- The [important consumer](datamodel/topic.md#important-consumer) flag for consumers used by streaming queries.
- [Autopartitioning](datamodel/topic.md#autopartitioning) (partition split/merge) for topics used by streaming queries. If the number of partitions in a topic read by a running streaming query increases, new partitions will not be processed.
- Changing the text of a running query. To change the query, recreate it — use [DROP STREAMING QUERY](../yql/reference/syntax/drop-streaming-query.md) and [CREATE STREAMING QUERY](../yql/reference/syntax/create-streaming-query.md).

## Management {#control}

Streaming queries are created, altered, and dropped with YQL statements:

- [CREATE STREAMING QUERY](../yql/reference/syntax/create-streaming-query.md) — create;
- [ALTER STREAMING QUERY](../yql/reference/syntax/alter-streaming-query.md) — alter and manage state;
- [DROP STREAMING QUERY](../yql/reference/syntax/drop-streaming-query.md) — drop.

Query state is available in the system table [`.sys/streaming_queries`](../dev/system-views.md#streaming).

## Query language {#syntax}

Streaming queries are written in [YQL](../yql/reference/index.md) and support familiar SQL constructs: [SELECT](../yql/reference/syntax/select/index.md), [WHERE](../yql/reference/syntax/select/where.md), [GROUP BY](../yql/reference/syntax/select/group-by.md), [JOIN](../yql/reference/syntax/select/join.md). [GROUP BY HOP](../yql/reference/syntax/select/group-by.md#group-by-hop) is used for time windows{% if feature_match_recogznize==true %}; [MATCH_RECOGNIZE](../yql/reference/syntax/select/match_recognize.md) for pattern matching{% endif %}.

## See also

- [{#T}](../dev/streaming-query/guarantees.md) — guarantees, windowed aggregation anomalies, and recommendations;
- [{#T}](../recipes/streaming_queries/topics.md) — step-by-step guide;
- [{#T}](../dev/streaming-query/streaming-query-formats.md) — data formats.
