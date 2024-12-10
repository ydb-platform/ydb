# Metrics reference

This page describes {{ ydb-short-name }} metrics. For information about Grafana dashboards that use these metrics, see [{#T}](./grafana-dashboards.md).

## Resource usage metrics {#resources}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| ----- | ----- |
| `resources.storage.used_bytes`<br/>`IGAUGE`, bytes | The size of user and service data stored in distributed network storage. `resources.storage.used_bytes` = `resources.storage.table.used_bytes` + `resources.storage.topic.used_bytes`. |
| `resources.storage.table.used_bytes`<br/>`IGAUGE`, bytes | The size of user and service data stored by tables in distributed network storage. Service data includes the data of the primary and [secondary indexes](../../../concepts/secondary_indexes.md). |
| `resources.storage.topic.used_bytes`<br/>`IGAUGE`, bytes | The size of storage used by topics. This metric sums the `topic.storage_bytes` values of all topics. |
| `resources.storage.limit_bytes`<br/>`IGAUGE`, bytes | A limit on the size of user and service data that a database can store in distributed network storage. |

## Common gRPC API metrics {#grpc_api}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| ----- | ----- |
| `api.grpc.request.bytes`<br/>`RATE`, bytes | The size of queries received by the database in a certain period of time.<br/>Labels:<br/>- _api_service_: The name of the gRPC API service, such as `table`.<br/>- _method_: The name of a gRPC API service method, such as `ExecuteDataQuery`. |
| `api.grpc.request.dropped_count`<br/>`RATE`, pieces | The number of requests dropped at the transport (gRPC) layer due to an error.<br/>Labels:<br/>- _api_service_: The name of the gRPC API service, such as `table`.<br/>- _method_: The name of a gRPC API service method, such as `ExecuteDataQuery`. |
| `api.grpc.request.inflight_count`<br/>`IGAUGE`, pieces | The number of requests that a database is simultaneously handling in a certain period of time.<br/>Labels:<br/>- _api_service_: The name of the gRPC API service, such as `table`.<br/>- _method_: The name of a gRPC API service method, such as `ExecuteDataQuery`. |
| `api.grpc.request.inflight_bytes`<br/>`IGAUGE`, bytes | The size of requests that a database is simultaneously handling in a certain period of time.<br/>Labels:<br/>- _api_service_: The name of the gRPC API service, such as `table`.<br/>- _method_: The name of a gRPC API service method, such as `ExecuteDataQuery`. |
| `api.grpc.response.bytes`<br/>`RATE`, bytes | The size of responses sent by the database in a certain period of time.<br/>Labels:<br/>- _api_service_: The name of the gRPC API service, such as `table`.<br/>- _method_: The name of a gRPC API service method, such as `ExecuteDataQuery`. |
| `api.grpc.response.count`<br/>`RATE`, pieces | The number of responses sent by the database in a certain period of time.<br/>Labels:<br/>- _api_service_: The name of the gRPC API service, such as `table`.<br/>- _method_: The name of a gRPC API service method, such as `ExecuteDataQuery`.<br/>- _status_ is the request execution status. See a more detailed description of statuses under [Error Handling](../../../reference/ydb-sdk/error_handling.md). |
| `api.grpc.response.dropped_count`<br/>`RATE`, pieces | The number of responses dropped at the transport (gRPC) layer due to an error.<br/>Labels:<br/>- _api_service_: The name of the gRPC API service, such as `table`.<br/>- _method_: The name of a gRPC API service method, such as `ExecuteDataQuery`. |
| `api.grpc.response.issues`<br/>`RATE`, pieces | The number of errors of a certain type arising in the execution of a request over a certain period of time.<br/>Tags:<br/>- _issue_type_ is the error type wth the only value being `optimistic_locks_invalidation`. For more on lock invalidation, review [Transactions and requests to {{ ydb-short-name }}](../../../concepts/transactions.md). |

## GRPC API metrics for topics {#grpc_api_topics}

#|
||
Metric name
Type, units of measurement
|
Description
Labels
||


||

`grpc.topic.stream_read.commits`
`RATE`, pieces
|
The number of commits (reading confirmations) after processing messages that were read by the `Ydb::TopicService::StreamRead` method.

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`grpc.topic.stream_read.bytes`
`RATE`, pieces
|
The number of bytes read by the `Ydb::TopicService::StreamRead` method.

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`grpc.topic.stream_read.messages`
`RATE`, pieces
|
The number of messages read by the `Ydb::TopicService::StreamRead` method.

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`grpc.topic.stream_read.partition_session.errors`
`RATE`, pieces
|
The number of errors occurred when working with the partition.

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`grpc.topic.stream_read.partition_session.started`
`RATE`, pieces
|
The number of sessions opened within the given unit of time.

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`grpc.topic.stream_read.partition_session.stopped`
`RATE`, pieces
|
The number of sessions closed within the given unit of time.

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`grpc.topic.stream_read.partition_session.starting_count`
`RATE`, pieces
|
The number of sessions in the process of opening (the client received the command to open a session, but did not open the session yet).

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`grpc.topic.stream_read.partition_session.stopping_count`
`RATE`, pieces
|
The number of sessions in the process of closing.

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`grpc.topic.stream_read.partition_session.count`
`RATE`, pieces
|
The number of active partition sessions.

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`grpc.topic.stream_write.bytes`
`RATE`, bytes
|
The number of bytes written by the `Ydb::TopicService::StreamWrite` method.

Labels:

- _topic_ – the name of the topic.

||

||

`grpc.topic.stream_write.uncommitted_bytes`
`RATE`, bytes
|
The number of bytes written by the `Ydb::TopicService::StreamWrite` method in uncommitted transactions.

Labels:

- _topic_ – the name of the topic.

||

||

`grpc.topic.stream_write.errors`
`RATE`, pieces
|
The number of errors that occurred when running the `Ydb::TopicService::StreamWrite` method.

Labels:

- _topic_ – the name of the topic.

||

||

`grpc.topic.stream_write.messages`
`RATE`, pieces
|
The number of messages written by the `Ydb::TopicService::StreamWrite` method.

Labels:

- _topic_ – the name of the topic.

||

||

`grpc.topic.stream_write.uncommitted_messages`
`RATE`, pieces
|
The number of messages written by the `Ydb::TopicService::StreamWrite` method in uncommitted transactions.

Labels:

- _topic_ – the name of the topic.

||

||

`grpc.topic.stream_write.partition_throttled_milliseconds`
`HIST_RATE`, pieces
|
Histogram counter. The intervals are specified in milliseconds. The number of throttled messages.

Labels:

- _topic_ – the name of the topic.

||

||

`grpc.topic.stream_write.sessions_active_count`
`GAUGE`, pieces
|
The number of sessions that are open for writing.

Labels:

- _topic_ – the name of the topic.

||

||

`grpc.topic.stream_write.sessions_created`
`RATE`, pieces
|
The number of sessions created for writing.

Labels:

- _topic_ – the name of the topic.

||
|#


## HTTP API metrics {#http_api}

#|
||
Metric name
Type, units of measurement
|
Description
Labels
||

||

`api.http.data_streams.request.count`
`RATE`, pieces
|
The number of HTTP requests.

Labels:

- _method_ – the name of the HTTP API service method, for example `PutRecord`, `GetRecords`.
- _topic_ – the name of the topic.

||

||

`api.http.data_streams.request.bytes`
`RATE`, bytes
|
The total size of HTTP requests.

Labels:

- _method_ – the name of the HTTP API service method, in this case only `PutRecord`.
- _topic_ – the name of the topic.

||

||

`api.http.data_streams.response.count`
`RATE`, pieces
|
The number of HTTP responses.

Labels:

- _method_ – the name of the HTTP API service method, for example `PutRecord`, `GetRecords`.
- _topic_ – the name of the topic.
- _code_ – HTTP response code.

||

||

`api.http.data_streams.response.bytes`
`RATE`, bytes
|
The total size of HTTP responses.

Labels:

- _method_ – the name of the HTTP API service method, in this case only `GetRecords`.
- _topic_ – the name of the topic.

||

||

`api.http.data_streams.response.duration_milliseconds`
`HIST_RATE`, pieces
|
Histogram counter. Intervals are specified in milliseconds. The number of responses, which execution time falls within the specified interval.

Labels:

- _method_ – the name of the HTTP API service method.
- _topic_ – the name of the topic.

||

||

`api.http.data_streams.get_records.messages`
`RATE`, pieces
|
The number of messages read by the `GetRecords` method.

Labels:

- _topic_ – the name of the topic.

||

||

`api.http.data_streams.put_record.messages`
`RATE`, pieces
|
The number of messages written by the `PutRecord` method (always equals 1).

Labels:

- _topic_ – the name of the topic.

||

||

`api.http.data_streams.put_records.failed_messages`
`RATE`, pieces
|
The number of failed (not written) messages sent by the `PutRecords` method.

Labels:

- _topic_ – the name of the topic.

||

||

`api.http.data_streams.put_records.successful_messages`
`RATE`, pieces
|
The number of successfully written messages that were sent by the `PutRecords` method.

Labels:

- _topic_ – the name of the topic.

||

||

`api.http.data_streams.put_records.total_messages`
`RATE`, pieces
|
The total number of messages sent by the `PutRecords` method.

Labels:

- _topic_ – the name of the topic.

||
|#

## Kafka API metrics {#kafka_api}

#|
||
Metric name
Type, units of measurement
|
Description
Labels
||

||

`api.kafka.request.count`
`RATE`, pieces
|
The number of Kafka-protocol requests per unit of time.

Labels:

- _method_ – the name of the Kafka API service method, for example `PRODUCE`, `SASL_HANDSHAKE`.

||

||

`api.kafka.request.bytes`
`RATE`, bytes
|
The total size of Kafka-protocol requests per unit of time.

Labels:

- _method_ – the name of the Kafka API service method, for example `PRODUCE`, `SASL_HANDSHAKE`.

||

||

`api.kafka.response.count`
`RATE`, pieces
|
The number of Kafka-protocol responses per unit of time.

Labels:

- _method_ – the name of the Kafka API service method, for example `PRODUCE`, `SASL_HANDSHAKE`.
- _error_code_ – Kafka response code.

||

||

`api.kafka.response.bytes`
`RATE`, bytes
|
The total size of Kafka-protocol responses per unit of time.

Labels:

- _method_ – the name of the Kafka API service method, for example `PRODUCE`, `SASL_HANDSHAKE`.

||

||

`api.kafka.response.duration_milliseconds`
`HIST_RATE`, pieces
|
Histogram counter. A set of intervals, in milliseconds, with the number of requests processed within these intervals.

Labels:

- _method_ – the name of the Kafka API service method.

||

||

`api.kafka.produce.failed_messages`
`RATE`, pieces
|
The number of failed (not written) messages sent by the `PRODUCE` method per unit of time.

Labels:

- _topic_ – the name of the topic.

||

||

`api.kafka.produce.successful_messages`
`RATE`, pieces
|
The number of successfully written messages sent by the `PRODUCE` method per unit of time.

Labels:

- _topic_ – the name of the topic.

||

||

`api.kafka.produce.total_messages`
`RATE`, pieces
|
The total number of the messages sent by the `PRODUCE` method per unit of time.

Labels:

- _topic_ – the name of the topic.

||
|#


## Session metrics {#sessions}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| ----- | ----- |
| `table.session.active_count`<br/>`IGAUGE`, pieces | The number of sessions started by clients and running at a given time. |
| `table.session.closed_by_idle_count`<br/>`RATE`, pieces | The number of sessions closed by the DB server in a certain period of time due to exceeding the lifetime allowed for an idle session. |

## Transaction processing metrics {#transactions}

You can analyze a transaction's execution time using a histogram counter. The intervals are set in milliseconds. The chart shows the number of transactions whose duration falls within a certain time interval.

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| ----- | ----- |
| `table.transaction.total_duration_milliseconds`<br/>`HIST_RATE`, pieces | The number of transactions with a certain duration on the server and client. The duration of a transaction is counted from the point of its explicit or implicit start to committing changes or its rollback. Includes the transaction processing time on the server and the time on the client between sending different requests within the same transaction.<br/>Labels:<br/>- _tx_kind_: The transaction type, possible values are `read_only`, `read_write`, `write_only`, and `pure`. |
| `table.transaction.server_duration_milliseconds`<br/>`HIST_RATE`, pieces | The number of transactions with a certain duration on the server. The duration is the time of executing requests within a transaction on the server. Does not include the waiting time on the client between sending separate requests within a single transaction.<br/>Labels:<br/> -_tx_kind_: The transaction type, possible values are`read_only`, `read_write`, `write_only`, and `pure`. |
| `table.transaction.client_duration_milliseconds`<br/>`HIST_RATE`, pieces | The number of transactions with a certain duration on the client. The duration is the waiting time on the client between sending individual requests within a single transaction. Does not include the time of executing requests on the server.<br/>Labels:<br/>- _tx_kind_: The transaction type, possible values are `read_only`, `read_write`, `write_only`, and `pure`. |

## Query processing metrics {#queries}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| ----- | ----- |
| `table.query.request.bytes`<br/>`RATE`, bytes | The size of YQL query text and parameter values to queries received by the database in a certain period of time. |
| `table.query.request.parameters_bytes`<br/>`RATE`, bytes | The parameter size to the queries received by the database in a certain period of time. |
| `table.query.response.bytes`<br/>`RATE`, bytes | The size of responses sent by the database in a certain period of time. |
| `table.query.compilation.latency_milliseconds`<br/>`HIST_RATE`, pieces | Histogram counter. The intervals are set in milliseconds. Shows the number of successfully executed compilation queries whose duration falls within a certain time interval. |
| `table.query.compilation.active_count`<br/>`IGAUGE`, pieces | The number of active compilations at a given time. |
| `table.query.compilation.count`<br/>`RATE`, pieces | The number of compilations that completed successfully in a certain time period. |
| `table.query.compilation.errors`<br/>`RATE`, pieces | The number of compilations that failed in a certain period of time. |
| `table.query.compilation.cache_hits`<br/>`RATE`, pieces | The number of queries in a certain period of time, which didn't require any compilation, because there was an existing plan in the cache of prepared queries. |
| `table.query.compilation.cache_misses`<br/>`RATE`, pieces | The number of queries in a certain period of time that required query compilation. |
| `table.query.execution.latency_milliseconds`<br/>`HIST_RATE`, pieces | Histogram counter. The intervals are set in milliseconds. Shows the number of queries whose execution time falls within a certain interval. |

## Table partition metrics {#datashards}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| ----- | ----- |
| `table.datashard.row_count`<br/>`GAUGE`, pieces | The number of rows in DB tables. |
| `table.datashard.size_bytes`<br/>`GAUGE`, bytes | The size of data in DB tables. |
| `table.datashard.used_core_percents`<br/>`HIST_GAUGE`, % | Histogram counter. The intervals are set as a percentage. Shows the number of table partitions using computing resources in the ratio that falls within a certain interval. |
| `table.datashard.read.rows`<br/>`RATE`, pieces | The number of rows that are read by all partitions of all DB tables in a certain period of time. |
| `table.datashard.read.bytes`<br/>`RATE`, bytes | The size of data that is read by all partitions of all DB tables in a certain period of time. |
| `table.datashard.write.rows`<br/>`RATE`, pieces | The number of rows that are written by all partitions of all DB tables in a certain period of time. |
| `table.datashard.write.bytes`<br/>`RATE`, bytes | The size of data that is written by all partitions of all DB tables in a certain period of time. |
| `table.datashard.scan.rows`<br/>`RATE`, pieces | The number of rows that are read through `StreamExecuteScanQuery` or `StreamReadTable` gRPC API calls by all partitions of all DB tables in a certain period of time. |
| `table.datashard.scan.bytes`<br/>`RATE`, bytes | The size of data that is read through `StreamExecuteScanQuery` or `StreamReadTable` gRPC API calls by all partitions of all DB tables in a certain period of time. |
| `table.datashard.bulk_upsert.rows`<br/>`RATE`, pieces | The number of rows that are added through a `BulkUpsert` gRPC API call to all partitions of all DB tables in a certain period of time. |
| `table.datashard.bulk_upsert.bytes`<br/>`RATE`, bytes | The size of data that is added through a `BulkUpsert` gRPC API call to all partitions of all DB tables in a certain period of time. |
| `table.datashard.erase.rows`<br/>`RATE`, pieces | The number of rows deleted from the database in a certain period of time. |
| `table.datashard.erase.bytes`<br/>`RATE`, bytes | The size of data deleted from the database in a certain period of time. |

## Resource usage metrics (for Dedicated mode only) {#ydb_dedicated_resources}

| Metric name<br/>Type<br/>units of measurement | Description<br/>Labels |
| ----- | ----- |
| `resources.cpu.used_core_percents`<br/>`RATE`, % | CPU usage. If the value is `100`, one of the cores is being used for 100%. The value may be greater than `100` for multi-core configurations.<br/>Labels:<br/>- _pool_: The computing pool, possible values are `user`, `system`, `batch`, `io`, and `ic`. |
| `resources.cpu.limit_core_percents`<br/>`IGAUGE`, % | The percentage of CPU available to a database. For example, for a database that has three nodes with four cores in `pool=user` per node, the value of this metric will be `1200`.<br/>Labels:<br/>- _pool_: The computing pool, possible values are `user`, `system`, `batch`, `io`, and `ic`. |
| `resources.memory.used_bytes`<br/>`IGAUGE`, bytes | The amount of RAM used by the database nodes. |
| `resources.memory.limit_bytes`<br/>`IGAUGE`, bytes | RAM available to the database nodes. |

## Query processing metrics (for Dedicated mode only) {#ydb_dedicated_queries}

| Metric name<br/>Type<br/>units of measurement | Description<br/>Labels |
| ----- | ----- |
| `table.query.compilation.cache_evictions`<br/>`RATE`, pieces | The number of queries evicted from the cache of prepared queries in a certain period of time. |
| `table.query.compilation.cache_size_bytes`<br/>`IGAUGE`, bytes | The size of the cache of prepared queries. |
| `table.query.compilation.cached_query_count`<br/>`IGAUGE`, pieces | The size of the cache of prepared queries. |

## Topic metrics {#topics}

| Metric name<br/>Type<br/>units of measurement | Description<br/>Labels |
| ----- | ----- |
|`topic.producers_count`<br/>`GAUGE`, pieces | The number of unique topic [producers](../../../concepts/topic#producer-id).<br/>Labels:<br/>- _topic_ – the name of the topic. |
| `topic.storage_bytes`<br/>`GAUGE`, bytes | The size of the topic in bytes. <br/>Labels:<br/>- _topic_ - the name of the topic. |
| `topic.read.bytes`<br/>`RATE`, bytes | The number of bytes read by the consumer from the topic.<br/>Labels:<br/>- _topic_ – the name of the topic.<br/>- _consumer_ – the name of the consumer. |
| `topic.read.messages`<br/>`RATE`, pieces | The number of messages read by the consumer from the topic. <br/>Labels:<br/>- _topic_ – the name of the topic.<br/>- _consumer_ – the name of the consumer. |
| `topic.read.lag_messages`<br/>`RATE`, pieces | The number of unread messages by the consumer in the topic.<br/>Labels:<br/>- _topic_ – the name of the topic.<br/>- _consumer_ – the name of the consumer. |
| `topic.read.lag_milliseconds`<br/>`HIST_RATE`, pieces | A histogram counter. The intervals are specified in milliseconds. It shows the number of messages where the difference between the reading time and the message creation time falls within the specified interval.<br/>Labels:<br/>- _topic_ – the name of the topic.<br/>- _consumer_ – the name of the consumer. |
| `topic.write.bytes`<br/>`RATE`, bytes | The size of the written data.<br/>Labels:<br/>- _topic_ – the name of the topic. |
| `topic.write.uncommited_bytes`<br/>`RATE`, bytes | The size of data written as part of ongoing transactions.<br/>Labels:<br/>- _topic_ — the name of the topic. |
| `topic.write.uncompressed_bytes`<br/>`RATE`, bytes | The size of uncompressed written data.<br/>Метки:<br/>- _topic_ – the name of the topic.
| `topic.write.messages`<br/>`RATE`, pieces | The number of written messages.<br/>Labels:<br/>- _topic_ – the name of the topic. |
| `topic.write.uncommitted_messages`<br/>`RATE`, pieces | The number of messages written as part of ongoing transactions.<br/>Labels:<br/>- _topic_ — the name of the topic. |
| `topic.write.message_size_bytes`<br/>`HIST_RATE`, pieces | A histogram counter. The intervals are specified in bytes. It shows the number of messages which size falls within the boundaries of the interval.<br/>Labels:<br/>- _topic_ – the name of the topic. |
| `topic.write.lag_milliseconds`<br/>`HIST_RATE`, pieces | A histogram counter. The intervals are specified in milliseconds. It shows the number of messages where the difference between the write time and the message creation time falls within the specified interval.<br/>Labels:<br/>- _topic_ – the name of the topic. |


## Aggregated topic partition metrics {#topics_partitions}

The following table describes aggregated metrics for topic partitions. The maximum and minimum values are calculated for all partitions of a specified topic.

#|

||
Metric name
Type, units of measurement
|
Description
Labels
||

||

`topic.partition.init_duration_milliseconds_max`
`GAUGE`, milliseconds
|
The maximum duration of topic partition initialization.

Labels:

- _topic_ – the name of the topic.

||

||

`topic.partition.producers_count_max`
`GAUGE`, pieces
|
The maximum number of producers in a topic partition.

Labels:

- _topic_ – the name of the topic.

||

||

`topic.partition.storage_bytes_max`
`GAUGE`, bytes
|
The maximum size of a topic partition.

Labels:

- _topic_ – the name of the topic.

||

||

`topic.partition.uptime_milliseconds_min`
`GAUGE`, pieces
|
The minimum uptime of a topic partition after a restart.

`topic.partition.uptime_milliseconds_min` is close to 0 during a rolling restart. After the rolling restart is completed, the `topic.partition.uptime_milliseconds_min` value should increase indefinitely.

Labels:

- _topic_ – the name of the topic.

||

||

`topic.partition.total_count`
`GAUGE`, pieces
|
The total number of partitions in a topic.

Labels:

- _topic_ – the name of the topic.

||

||

`topic.partition.alive_count`
`GAUGE`, pieces
|
The number of partitions that send their metrics.

Labels:

- _topic_ – the name of the topic.

||

||

`topic.partition.committed_end_to_end_lag_milliseconds_max`
`GAUGE`, milliseconds
|
The maximum difference (in all topic partitions) between the current time and the time when the last committed message was created.

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`topic.partition.committed_lag_messages_max`
`GAUGE`, pieces
|
The maximum difference (in all topic partitions) between the last partition offset and the committed partition offset.

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`topic.partition.committed_read_lag_milliseconds_max`
`GAUGE`, milliseconds
|
The maximum difference (in all topic partitions) between the current time and the time when the last committed message was written.

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`topic.partition.end_to_end_lag_milliseconds_max`
`GAUGE`, milliseconds
|
<!--oldest or newest ??? -->
The difference between the current time and the oldest message among all of the messages that were read for the last minute in all topic partitions.

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`topic.partition.lag_messages_max`
`GAUGE`, pieces
|
The maximum difference (in all topic partitions) between the last offset in a partition and the last read offset.

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`topic.partition.read.lag_milliseconds_max`
`GAUGE`, milliseconds
|
The difference between the current time and the minimum write time among all of the messages read for the last minute in all partitions.

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`topic.partition.read.idle_milliseconds_max`
`GAUGE`, milliseconds
|
The maximum idle time (the time since the last read operation from a partition) in all partitions.

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`topic.partition.read.lag_milliseconds_max`
`GAUGE`, milliseconds
|
The maximum difference between the write time and the creation time among all of the messages that were read for the last minute.

Labels:

- _topic_ – the name of the topic.
- _consumer_ – the name of the consumer.

||

||

`topic.partition.write.lag_milliseconds_max`
`GAUGE`, milliseconds
|
The maximum difference between the write time and the creation time among all of the messages that were written for the last minute.

Labels:

- _topic_ – the name of the topic.

||

||
`topic.partition.write.speed_limit_bytes_per_second`
`GAUGE`, bytes per second
|
The limit for the writing speed to a single partition.

Labels:

- _topic_ – the name of the topic.

||

||

`topic.partition.write.throttled_nanoseconds_max`
`GAUGE`, nanoseconds
|
The maximum throttled time for writing operations by all partitions. The worst case scenario, `topic.partition.write.throttled_nanoseconds_maх` = 10^9, means that writing was throttled for the whole second.

Labels:

- _topic_ – the name of the topic.

||

||

`topic.partition.write.bytes_per_day_max`
`GAUGE`, bytes
|
The maximum number of bytes written for the last day in all topic partitions.

Labels:

- _topic_ – the name of the topic.

||

||

`topic.partition.write.bytes_per_hour_max`
`GAUGE`, bytes
|
The maximum number of bytes written for the last hour in all topic partitions.

Labels:

- _topic_ – the name of the topic.

||

||

`topic.partition.write.bytes_per_minute_max`
`GAUGE`, bytes
|
The maximum number of bytes written for the last minute in all topic partitions.

Labels:

- _topic_ – the name of the topic.

||

||

`topic.partition.write.idle_milliseconds_max`
`GAUGE`, milliseconds
|
The maximum idle time of a partition for writing.

Labels:

- _topic_ – the name of the topic.

||

|#


## Resource pool metrics {#resource_pools}

#|
||
Metric name
Type, units of measurement
|
Description
||

||
`kqp.workload_manager.CpuQuotaManager.AverageLoadPercentage`
`RATE`, pieces
|
Average database load. `DATABASE_LOAD_CPU_THRESHOLD` uses this metric.
||

||
`kqp.workload_manager.InFlightLimit`
`GAUGE`, pieces
|
The limit on the number of simultaneous (in flight) requests.
||

||
`kqp.workload_manager.GlobalInFly`
`GAUGE`, pieces
|
The number of requests that a database is handling at the moment. This metric works only for the pools with the enabled `CONCURRENT_QUERY_LIMIT` or `DATABASE_LOAD_CPU_THRESHOLD` option.
||

||
`kqp.workload_manager.QueueSizeLimit`
`GAUGE`, pieces
|
The size of the execution queue for the requests that await processing.
||

||
`kqp.workload_manager.GlobalDelayedRequests`
`GAUGE`, pieces
|
The number of requests in the execution queue. This metric works only for the pools with the enabled `CONCURRENT_QUERY_LIMIT` or `DATABASE_LOAD_CPU_THRESHOLD` option.
||
|#
