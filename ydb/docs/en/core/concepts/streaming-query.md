# Streaming queries

A **streaming query** is a query type designed for continuous processing of unbounded data ([stream processing](https://en.wikipedia.org/wiki/Stream_processing)). Unlike regular queries, a streaming query does not finish after returning a result; it keeps running and processes data as it arrives.

Stream processing is widely used in systems such as [Apache Flink](https://flink.apache.org/), [Apache Kafka Streams](https://kafka.apache.org/documentation/streams/), and [Amazon Kinesis Data Analytics](https://aws.amazon.com/kinesis/data-analytics/). Typical use cases include monitoring and alerting, aggregating metrics over time windows, transforming and filtering events on the fly, enriching events from lookup tables, and detecting patterns in event sequences.

{{ ydb-short-name }} implements stream processing as part of a unified data platform. Beyond typical streaming scenarios, integration into {{ ydb-short-name }} lets you ingest data from [topics](datamodel/topic.md), process change streams from [row-oriented tables](datamodel/table.md#row-oriented-tables) via [CDC](cdc.md), and write results to output topics or directly into tables. That way you can build low-latency data pipelines entirely inside {{ ydb-short-name }}.

## Differences from regular queries {#differences}

Regular queries operate on data already stored in tables. The query runs, returns a result, and completes. A streaming query is created and keeps running indefinitely until explicitly stopped by the user. Data continuously arrives in a topic, flows through the query, and is written to a sink—another topic or a table.


| Characteristic | Regular queries | Streaming queries |
|----------------|-----------------|-------------------|
| Data | Finite sets in tables | Unbounded event streams |
| Lifetime | Completes after processing | Runs continuously |
| Result | Available after completion | Updates as data arrives |
| Recovery from failures | Manual restart | Automatic recovery from a [checkpoint](../dev/streaming-query/checkpoints.md) |

## Data sources and sinks {#data-flow}

Streaming queries read data from {{ ydb-short-name }} [topics](datamodel/topic.md) and write results to [topics](datamodel/topic.md) or [tables](datamodel/table.md). Data may arrive in a topic from external systems (for example, an application writes events using an SDK), but the streaming query itself only works with {{ ydb-short-name }} entities. Direct reads from external systems such as Apache Kafka, or writes to other databases such as PostgreSQL, are not supported.

### Sources {#sources}

**Topics** are the primary source of streaming data. The query reads messages from one or several [topics](datamodel/topic.md) and processes them as they arrive. Data can be written to a topic by:

* **External applications** — via the [{{ ydb-short-name }} SDK](../reference/ydb-sdk/index.md) or the [Kafka API](../reference/kafka-api/index.md). For example, a service sends telemetry or logs into a {{ ydb-short-name }} topic, and a streaming query processes them.
* **CDC (Change Data Capture)** — change streams from tables implemented using built-in [topics](datamodel/topic.md). They let you react to inserts, updates, and deletes in near real time. See [{#T}](cdc.md).

### Sinks {#sinks}

**Topics** — for handing off results to other systems or downstream processing stages.

**Tables** — for materializing results. Data is written with [UPSERT](../yql/reference/syntax/upsert_into.md) and is available to regular SQL queries.

## Guarantees {#guarantees}

Under normal operation, streaming queries provide [at-least-once](https://en.wikipedia.org/wiki/Reliable_messaging#At-least-once_delivery) delivery: if a failure occurs, the query recovers automatically from a [checkpoint](../dev/streaming-query/checkpoints.md) and resumes reading from saved offsets.

Important limitations of the current implementation:

- **Offset reset when recreating a query:** Changing the query text via [DROP](../yql/reference/syntax/drop-streaming-query.md) + [CREATE](../yql/reference/syntax/create-streaming-query.md) deletes the checkpoint. The new query starts reading from the end of the topic, skipping events that arrived between deleting the old version and starting the new one.
- **Incomplete aggregates without watermarks:** Because there is no [watermark](https://en.wikipedia.org/wiki/Watermark_(data_synchronization)) mechanism yet, time windows close based on wall-clock time. Events that arrive late (for example, due to slow partitions or network lag) may not be included in aggregates for windows that have already closed.

For a detailed discussion of guarantees, anomalies, and mitigation, see [{#T}](../dev/streaming-query/guarantees.md).

{% note info %}

We are actively improving stream processing. Delivery guarantees will get stronger in future releases.

{% endnote %}

## Limitations {#limitations}

{% note warning %}

- The query must contain at least one read from a topic, because streaming processing requires a continuous input stream.
- `JOIN` between two streams is not supported (a temporary architectural limitation).
- Enrichment from {{ ydb-short-name }} tables is not supported — streaming queries may only use [S3 external tables](query_execution/federated_query/s3/external_table.md) for enrichment (support for {{ ydb-short-name }} tables is planned for release 26.1).
- Reading and writing **local** topics directly is not supported — use [external data sources](datamodel/external_data_source.md) that point at the current database (this restriction is planned to be lifted in release 26.1).

{% endnote %}

To work with local topics, use [external data sources](datamodel/external_data_source.md):

- Create an [external data source](datamodel/external_data_source.md) pointing to the same or another {{ ydb-short-name }} database and access topics through it.

Also not supported in the current version:

- The [important consumer](datamodel/topic.md#important-consumer) flag for consumers used by streaming queries.
- [Autopartitioning](datamodel/topic.md#autopartitioning) (split/merge) for topics used by streaming queries. If the number of partitions in a topic that a running streaming query reads from increases, the new partitions will not be processed.
- Changing the text of a running query. To change the query, recreate it — delete it with [DROP STREAMING QUERY](../yql/reference/syntax/drop-streaming-query.md) and create it again with [CREATE STREAMING QUERY](../yql/reference/syntax/create-streaming-query.md).

## Control {#control}

Streaming queries are created, modified, and deleted using YQL commands:

- [CREATE STREAMING QUERY](../yql/reference/syntax/create-streaming-query.md) — create;
- [ALTER STREAMING QUERY](../yql/reference/syntax/alter-streaming-query.md) — change settings and control lifecycle;
- [DROP STREAMING QUERY](../yql/reference/syntax/drop-streaming-query.md) — delete.

Query state is available in the system table [`.sys/streaming_queries`](../dev/system-views.md#streaming).

## Query language {#syntax}

Streaming queries are written in [YQL](../yql/reference/index.md) and support familiar SQL constructs: [SELECT](../yql/reference/syntax/select/index.md), [WHERE](../yql/reference/syntax/select/where.md), [GROUP BY](../yql/reference/syntax/select/group-by.md), [JOIN](../yql/reference/syntax/select/join.md). For time windows, use [GROUP BY HOP](../yql/reference/syntax/select/group-by.md#group-by-hop){% if feature_match_recogznize==true %}; for pattern matching, [MATCH_RECOGNIZE](../yql/reference/syntax/select/match_recognize.md){% endif %}.

## See also

- [{#T}](../dev/streaming-query/guarantees.md) — guarantees, windowing anomalies, and recommendations;
- [{#T}](../recipes/streaming_queries/topics.md) — step-by-step tutorial;
- [{#T}](../dev/streaming-query/streaming-query-formats.md) — data formats.
