# Metrics reference

This document provides a reference for the main {{ ydb-short-name }} system metrics used for monitoring cluster state, diagnosing performance, and analyzing load.

How to view current metric values in the [built-in web interface](../../../devops/observability/monitoring.md#web-metrics) and configure their collection in Prometheus and Grafana is described in the [Configuring monitoring of the {{ ydb-short-name }} cluster](../../../devops/observability/monitoring.md) section.

## Resource usage metrics {#resources}

| Metric name<br/>Type, units | Description<br/>Labels |
| --- | --- |
| `resources.storage.used_bytes`<br/>`IGAUGE`, bytes | Size of user and service data stored in the distributed network storage. `resources.storage.used_bytes` = `resources.storage.table.used_bytes` + `resources.storage.topic.used_bytes`. |
| `resources.storage.table.used_bytes`<br/>`IGAUGE`, bytes | Size of user and service data stored by tables in the distributed network storage. Service data includes data of primary, [secondary indexes](../../../concepts/glossary.md#secondary-index), [vector indexes](../../../concepts/glossary.md#vector-index), [full-text indexes](../../../concepts/glossary.md#fulltext-index), [local Bloom indexes](../../../concepts/glossary.md#local-bloom-skip-index), and [local min_max index](../../../concepts/glossary.md#local-min-max-index). |
| `resources.storage.topic.used_bytes`<br/>`IGAUGE`, bytes | Size of the distributed network storage used by topics. Equals the sum of `topic.storage_bytes` values of all topics. |
| `resources.storage.limit_bytes`<br/>`IGAUGE`, bytes | Limit on the size of user and service data that the database can store in the distributed network storage. |

## Common gRPC API metrics {#api}

| Metric name<br/>Type, units | Description<br/>Labels |
| --- | --- |
| `api.grpc.request.bytes`<br/>`RATE`, bytes | Size of requests received by the database in a certain time period.<br/>Labels:<br/>- _api_service_ – name of the gRPC API service, for example `table` or `data_streams`.<br/>- _method_ – name of the gRPC API service method, for example `ExecuteDataQuery` (for the `table` service), or `PutRecord`, `GetRecords` (for the `data_streams` service). |
| `api.grpc.request.dropped_count`<br/>`RATE`, units | Number of requests whose processing was terminated at the transport (gRPC) level due to an error.<br/>Labels:<br/>- _api_service_ – name of the gRPC API service, for example `table`.<br/>- _method_ – name of the gRPC API service method, for example `ExecuteDataQuery`. |
| `api.grpc.request.inflight_count`<br/>`IGAUGE`, units | Number of requests being processed simultaneously by the database in a certain time period.<br/>Labels:<br/>- _api_service_ – name of the gRPC API service, for example `table`.<br/>- _method_ – name of the gRPC API service method, for example `ExecuteDataQuery`. |
| `api.grpc.request.inflight_bytes`<br/>`IGAUGE`, bytes | Size of requests being processed simultaneously by the database in a certain time period.<br/>Labels:<br/>- _api_service_ – name of the gRPC API service, for example `table`.<br/>- _method_ – name of the gRPC API service method, for example `ExecuteDataQuery`. |
| `api.grpc.response.bytes`<br/>`RATE`, bytes | Size of responses sent by the database in a certain time period.<br/>Labels:<br/>- _api_service_ – name of the gRPC API service, for example `table`.<br/>- _method_ – name of the gRPC API service method, for example `ExecuteDataQuery`. |
| `api.grpc.response.count`<br/>`RATE`, units | Number of responses sent by the database in a certain time period.<br/>Labels:<br/>- _api_service_ – name of the gRPC API service, for example `table`.<br/>- _method_ – name of the gRPC API service method, for example `ExecuteDataQuery`.<br/>- _status_ – request execution status; for more details on statuses, see the [Error handling](../../../reference/ydb-sdk/error_handling.md) section. |
| `api.grpc.response.dropped_count`<br/>`RATE`, units | Number of responses whose sending was terminated at the transport (gRPC) level due to an error.<br/>Labels:<br/>- _api_service_ – name of the gRPC API service, for example `table`.<br/>- _method_ – name of the gRPC API service method, for example `ExecuteDataQuery`. |
| `api.grpc.response.issues`<br/>`RATE`, units | Number of errors of a certain type that occurred during query execution in a certain time period.<br/>Labels:<br/>- _issue_type_ – error type, the only value is `optimistic_locks_invalidation`; for more details on lock invalidation, see the [Transactions and queries to {{ ydb-short-name }}](../../../concepts/transactions.md) section. |

## gRPC API metrics for topics {#grpc_api_topics}

| Metric name<br/>Type, units | Description<br/>Labels |
| --- | --- |
| `grpc.topic.stream_read.commits`<br/>`RATE`, units | Number of commits of the `Ydb::TopicService::StreamRead` method.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name. |
| `grpc.topic.stream_read.bytes`<br/>`RATE`, units | Number of bytes read by the `Ydb::TopicService::StreamRead` method.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name. |
| `grpc.topic.stream_read.messages`<br/>`RATE`, units | Number of messages read by the `Ydb::TopicService::StreamRead` method.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name. |
| `grpc.topic.stream_read.partition_session.errors`<br/>`RATE`, units | Number of errors when working with a partition.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – reader name. |
| `grpc.topic.stream_read.partition_session.started`<br/>`RATE`, count | Number of sessions started per unit of time.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – reader name. |
| `grpc.topic.stream_read.partition_session.stopped`<br/>`RATE`, count | Number of sessions stopped per unit of time.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – reader name. |
| `grpc.topic.stream_read.partition_session.starting_count`<br/>`RATE`, count | Number of sessions being started (that is, the client received a command to start a session, but the client has not started the session yet).<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – reader name. |
| `grpc.topic.stream_read.partition_session.stopping_count`<br/>`RATE`, count | Number of sessions being stopped.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – reader name. |
| `grpc.topic.stream_read.partition_session.count`<br/>`RATE`, count | Number of partition_session.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – reader name. |
| `grpc.topic.stream_write.bytes`<br/>`RATE`, bytes | Number of bytes written by the `Ydb::TopicService::StreamWrite` method.<br/>Labels:<br/>- _topic_ – topic name. |
| `grpc.topic.stream_write.uncommitted_bytes`<br/>`RATE`, bytes | Number of bytes written by the `Ydb::TopicService::StreamWrite` method within uncommitted transactions.<br/>Labels:<br/>- _topic_ – topic name. |
| `grpc.topic.stream_write.errors`<br/>`RATE`, count | Number of errors when calling the `Ydb::TopicService::StreamWrite` method.<br/>Labels:<br/>- _topic_ – topic name. |
| `grpc.topic.stream_write.messages`<br/>`RATE`, count | Number of messages written by the `Ydb::TopicService::StreamWrite` method.<br/>Labels:<br/>- _topic_ – topic name. |
| `grpc.topic.stream_write.uncommitted_messages`<br/>`RATE`, count | Number of messages written by the `Ydb::TopicService::StreamWrite` method within uncommitted transactions.<br/>Labels:<br/>- _topic_ – topic name. |
| `grpc.topic.stream_write.partition_throttled_milliseconds`<br/>`HIST_RATE`, count | Histogram counter. Intervals are set in milliseconds. Shows the number of messages that waited on the quota.<br/>Labels:<br/>- _topic_ – topic name. |
| `grpc.topic.stream_write.sessions_active_count`<br/>`GAUGE`, count | Number of open write sessions.<br/>Labels:<br/>- _topic_ – topic name. |
| `grpc.topic.stream_write.sessions_created`<br/>`RATE`, count | Number of created write sessions.<br/>Labels:<br/>- _topic_ – topic name. |

## HTTP API metrics {#http_api}

| Metric name<br/>Type, units | Description<br/>Labels |
| --- | --- |
| `api.http.data_streams.request.count`<br/>`RATE`, count | Number of requests over HTTP.<br/>Labels:<br/>- _method_ – HTTP API service method name, e.g., `PutRecord`, `GetRecords`.<br/>- _topic_ – topic name. |
| `api.http.data_streams.request.bytes`<br/>`RATE`, bytes | Total size of requests over HTTP.<br/>Labels:<br/>- _method_ – HTTP API service method name, in this case only `PutRecord`.<br/>- _topic_ – topic name. |
| `api.http.data_streams.response.count`<br/>`RATE`, count | Number of responses over HTTP.<br/>Labels:<br/>- _method_ – HTTP API service method name, e.g., `PutRecord`, `GetRecords`.<br/>- _topic_ – topic name.<br/>- _code_ – HTTP response code. |
| `api.http.data_streams.response.bytes`<br/>`RATE`, bytes | Total size of responses over HTTP.<br/>Labels:<br/>- _method_ – HTTP API service method name, in this case only `GetRecords`.<br/>- _topic_ – topic name. |
| `api.http.data_streams.response.duration_milliseconds`<br/>`HIST_RATE`, count | Histogram counter. Intervals are set in milliseconds. Shows the number of responses whose execution time falls within a certain interval.<br/>Labels:<br/>- _method_ – HTTP API service method name.<br/>- _topic_ – topic name. |
| `api.http.data_streams.get_records.messages`<br/>`RATE`, count | Number of messages read by the `GetRecords` method.<br/>Labels:<br/>- _topic_ – topic name. |
| `api.http.data_streams.put_record.messages`<br/>`RATE`, count | Number of messages sent by the `PutRecord` method.<br/>Labels:<br/>- _topic_ – topic name. |
| `api.http.data_streams.put_records.failed_messages`<br/>`RATE`, count | Number of messages sent by the `PutRecords` method that were not written.<br/>Labels:<br/>- _topic_ – topic name. |
| `api.http.data_streams.put_records.successful_messages`<br/>`RATE`, count | Number of messages sent by the `PutRecords` method that were successfully written.<br/>Labels:<br/>- _topic_ – topic name. |
| `api.http.data_streams.put_records.total_messages`<br/>`RATE`, count | Number of messages sent by the `PutRecords` method.<br/>Labels:<br/>- _topic_ – topic name. |

## Kafka API metrics {#kafka_api}

| Metric name<br/>Type, units | Description<br/>Labels |
| --- | --- |
| `api.kafka.request.count`<br/>`RATE`, count | Number of requests over the Kafka protocol per unit time.<br/>Labels:<br/>- _method_ – Kafka API service method name, e.g., `PRODUCE`, `SASL_HANDSHAKE`. |
| `api.kafka.request.bytes`<br/>`RATE`, bytes | Total size of requests over the Kafka protocol per unit time.<br/>Labels:<br/>- _method_ – Kafka API service method name, e.g., `PRODUCE`, `SASL_HANDSHAKE`. |
| `api.kafka.response.count`<br/>`RATE`, count | Number of responses via the Kafka protocol per unit time.<br/>Labels:<br/>- _method_ – name of the Kafka API service method, e.g., `PRODUCE`, `SASL_HANDSHAKE`.<br/>- _error_code_ – Kafka response code. |
| `api.kafka.response.bytes`<br/>`RATE`, bytes | Total size of responses via the Kafka protocol per unit time.<br/>Labels:<br/>- _method_ – name of the Kafka API service method, e.g., `PRODUCE`, `SASL_HANDSHAKE`. |
| `api.kafka.response.duration_milliseconds`<br/>`HIST_RATE`, units | Histogram counter. Defines a set of intervals in milliseconds and for each of them shows the number of requests with execution time falling within that interval.<br/>Labels:<br/>- _method_ – Kafka API service method name. |
| `api.kafka.produce.failed_messages`<br/>`RATE`, units | Number of messages per unit time sent by the `PRODUCE` method that were not written.<br/>Labels:<br/>- _topic_ – topic name. |
| `api.kafka.produce.successful_messages`<br/>`RATE`, units | Number of messages per unit time sent by the `PRODUCE` method that were successfully written.<br/>Labels:<br/>- _topic_ – topic name. |
| `api.kafka.produce.total_messages`<br/>`RATE`, units | Number of messages per unit time sent by the `PRODUCE` method.<br/>Labels:<br/>- _topic_ – topic name. |

## Session metrics {#sessions}

| Metric name<br/>Type, units | Description<br/>Labels |
| --- | --- |
| `table.session.active_count`<br/>`IGAUGE`, units | Number of sessions currently open by clients. |
| `table.session.closed_by_idle_count`<br/>`RATE`, units | Number of sessions closed by the database server in a certain period of time due to exceeding the time allocated for an inactive session's lifetime. |

## Transaction processing metrics {#transactions}

Transaction execution duration can be analyzed using a histogram counter. Intervals are set in milliseconds. The chart shows the number of transactions whose duration falls within a certain time interval.

| Metric name<br/>Type, units | Description<br/>Labels |
| --- | --- |
| `table.transaction.total_duration_milliseconds`<br/>`HIST_RATE`, units | Number of transactions with a specific execution duration on the server and client. Execution duration is the time from the explicit or implicit start of a transaction to the commit or rollback. It includes the transaction processing time on the server and the time on the client between sending different requests within the same transaction.<br/>Labels:<br/>- _tx_kind_ – transaction type, possible values `read_only`, `read_write`, `write_only`, `pure`. |
| `table.transaction.server_duration_milliseconds`<br/>`HIST_RATE`, units | Number of transactions with a specific execution duration on the server. Execution duration is the time of executing queries within a transaction on the server. It does not include the waiting time on the client between sending individual requests in the same transaction.<br/>Labels:<br/>- _tx_kind_ – transaction type, possible values `read_only`, `read_write`, `write_only`, `pure`. |
| `table.transaction.client_duration_milliseconds`<br/>`HIST_RATE`, units | Number of transactions with a specific execution duration on the client. Execution duration is the waiting time on the client between sending individual requests in the same transaction. It does not include the execution time of queries on the server.<br/>Labels:<br/>- _tx_kind_ – transaction type, possible values `read_only`, `read_write`, `write_only`, `pure`. |

## Query processing metrics {#queries}

| Metric name<br/>Type, units | Description<br/>Labels |
| --- | --- |
| `table.query.request.bytes`<br/>`RATE`, bytes | Size of YQL query text and parameter values for queries that arrived at the database in a certain period of time. |
| `table.query.request.parameters_bytes`<br/>`RATE`, bytes | Size of parameters for queries that arrived at the database in a certain period of time. |
| `table.query.response.bytes`<br/>`RATE`, bytes | Size of responses sent by the database in a certain period of time. |
| `table.query.compilation.latency_milliseconds`<br/>`HIST_RATE`, units | Histogram counter. Intervals are set in milliseconds. Shows the number of successfully completed compilation requests whose duration falls within a certain time interval. |
| `table.query.compilation.active_count`<br/>`IGAUGE`, units | Number of compilations currently in progress. |
| `table.query.compilation.count`<br/>`RATE`, units | Number of compilations that completed successfully in a certain period of time. |
| `table.query.compilation.errors`<br/>`RATE`, units | Number of compilations that failed in a certain period of time. |
| `table.query.compilation.cache_hits`<br/>`RATE`, units | Number of queries in a certain period of time that did not require compiling the query because a previously created plan was in the prepared query cache. |
| `table.query.compilation.cache_misses`<br/>`RATE`, count | The number of queries in a given period of time that required compiling a query. |
| `table.query.execution.latency_milliseconds`<br/>`HIST_RATE`, count | Histogram counter. Intervals are specified in milliseconds. Shows the number of queries whose execution time falls within a certain interval. |

## Row table partition metrics {#datashards}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| --- | --- |
| `table.datashard.row_count`<br/>`GAUGE`, count | Number of rows in all row tables of the database. |
| `table.datashard.size_bytes`<br/>`GAUGE`, bytes | Size of data in all row tables of the database. |
| `table.datashard.used_core_percents`<br/>`HIST_GAUGE`, % | Histogram counter. Intervals are specified in percentages. Shows the number of partitions of row tables that use computing resources in a proportion that falls within a certain interval. |
| `table.datashard.read.rows`<br/>`RATE`, count | Number of rows read by all partitions of all row tables in the database during a given period of time. |
| `table.datashard.read.bytes`<br/>`RATE`, bytes | Size of data read by all partitions of all row tables in the database during a given period of time. |
| `table.datashard.write.rows`<br/>`RATE`, count | Number of rows written by all partitions of all row tables in the database during a given period of time. |
| `table.datashard.write.bytes`<br/>`RATE`, bytes | Size of data written by all partitions of all row tables in the database during a given period of time. |
| `table.datashard.scan.rows`<br/>`RATE`, count | Number of rows read via gRPC API calls `StreamExecuteScanQuery` or `StreamReadTable` by all partitions of all row tables in the database over a certain time period. |
| `table.datashard.scan.bytes`<br/>`RATE`, bytes | Size of data read via gRPC API call `StreamExecuteScanQuery` or `StreamReadTable` by all partitions of all row tables in the database over a certain time period. |
| `table.datashard.bulk_upsert.rows`<br/>`RATE`, count | Number of rows added via gRPC API call `BulkUpsert` to all partitions of all row tables in the database over a certain time period. |
| `table.datashard.bulk_upsert.bytes`<br/>`RATE`, bytes | Size of data added via gRPC API call `BulkUpsert` to all partitions of all row tables in the database over a certain time period. |
| `table.datashard.erase.rows`<br/>`RATE`, count | Number of rows deleted from all row tables in the database during a given period of time. |
| `table.datashard.erase.bytes`<br/>`RATE`, bytes | Size of data deleted from all row tables in the database during a given period of time. |
| `table.datashard.cache_hit.bytes`<br/>`RATE`, bytes | Total volume of row table data successfully retrieved from memory (cache). A larger volume of data retrieved from the cache indicates efficient cache usage without accessing the distributed storage. |
| `table.datashard.cache_miss.bytes`<br/>`RATE`, bytes | Total volume of row table data that was requested but not found in memory (cache) and was read from distributed storage. Indicates potential areas for cache optimization. |

## Replicas metrics {#followers}

| Metric name<br/>Type, unit | Description<br/>Labels |
| --- | --- |
| `FollowerSyncLatency`<br/>`HIST_RATE`, microseconds | The latency of applying updates on followers relative to their commit on the leader. <br/>Labels:<br/>- _type_ – tablet type, the only value is `DataShard`. |

## Columnar table partition metrics {#columnshards}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| --- | --- |
| `table.columnshard.write.rows`<br/>`RATE`, count | Number of rows written by all partitions of all columnar tables in the database during a given period of time. |
| `table.columnshard.write.bytes`<br/>`RATE`, bytes | Size of data written by all partitions of all columnar tables in the database during a given period of time. |
| `table.columnshard.scan.rows`<br/>`RATE`, count | Number of rows read via gRPC API calls `StreamExecuteScanQuery` or `StreamReadTable` by all partitions of all columnar tables in the database over a certain time period. |
| `table.columnshard.scan.bytes`<br/>`RATE`, bytes | Size of data read via gRPC API call `StreamExecuteScanQuery` or `StreamReadTable` by all partitions of all columnar tables in the database over a certain time period. |
| `table.columnshard.bulk_upsert.rows`<br/>`RATE`, count | Number of rows added via gRPC API call `BulkUpsert` to all partitions of all columnar tables in the database over a certain time period. |
| `table.columnshard.bulk_upsert.bytes`<br/>`RATE`, bytes | Size of data added via gRPC API call `BulkUpsert` to all partitions of all columnar tables in the database over a certain time period. |

## Resource usage metrics (Dedicated mode only) {#ydb_dedicated_resources}

| Metric name<br/>Type<br/>units of measurement | Description<br/>Labels |
| --- | --- |
| `resources.cpu.used_core_percents`<br/>`RATE`, % | CPU usage. A value of `100` means that one of the cores is 100% utilized. The value can be greater than `100` for configurations with more than 1 core.<br/>Labels:<br/>- _pool_ – compute pool, possible values `user`, `system`, `batch`, `io`, `ic`. |
| `resources.cpu.limit_core_percents`<br/>`IGAUGE`, % | CPU available to the database as a percentage. For example, for a database of three nodes with 4 cores in `pool=user` on each node, the value of this sensor will be `1200`.<br/>Labels:<br/>- _pool_ – compute pool, possible values `user`, `system`, `batch`, `io`, `ic`. |
| `resources.memory.used_bytes`<br/>`IGAUGE`, bytes | RAM used by database nodes. |
| `resources.memory.limit_bytes`<br/>`IGAUGE`, bytes | RAM available to database nodes. |

## Query processing metrics (Dedicated mode only) {#ydb_dedicated_queries}

| Metric name<br/>Type<br/>units of measurement | Description<br/>Labels |
| --- | --- |
| `table.query.compilation.cache_evictions`<br/>`RATE`, count | Number of queries evicted from the prepared query cache during a given period of time. |
| `table.query.compilation.cache_size_bytes`<br/>`IGAUGE`, bytes | Size of the prepared query cache. |
| `table.query.compilation.cached_query_count`<br/>`IGAUGE`, count | Number of queries in the prepared query cache. |

## Topic metrics {#topics}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| --- | --- |
| `topic.producers_count`<br/>`GAUGE`, count | Number of unique topic [sources](../../../concepts/datamodel/topic#producer-id).<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.storage_bytes`<br/>`GAUGE`, bytes | Topic size in bytes.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.read.bytes`<br/>`RATE`, bytes | Number of bytes read from the topic.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – reader name. |
| `topic.read.messages`<br/>`RATE`, count | Number of messages read from the topic.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – reader name. |
| `topic.read.lag_messages`<br/>`RATE`, count | Total number of messages not yet read by the given reader across the topic.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – reader name. |
| `topic.read.lag_milliseconds`<br/>`HIST_RATE`, count | Histogram counter. Intervals are specified in milliseconds. Shows the number of messages for which the difference between the read time and the message creation time falls within a given interval.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – reader name. |
| `topic.write.bytes`<br/>`RATE`, bytes | Size of written data.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.write.uncommited_bytes`<br/>`RATE`, bytes | Size of data written as part of not yet completed transactions.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.write.uncompressed_bytes`<br/>`RATE`, bytes | Size of decompressed written data.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.write.messages`<br/>`RATE`, count | Number of written messages.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.write.uncommitted_messages`<br/>`RATE`, count | Number of messages written as part of not yet completed transactions.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.write.message_size_bytes`<br/>`HIST_RATE`, count | Histogram counter. Intervals are specified in bytes. Shows the number of messages whose size matches the interval boundaries.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.write.lag_milliseconds`<br/>`HIST_RATE`, count | Histogram counter. Intervals are specified in milliseconds. Shows the number of messages for which the difference between the write time and the message creation time falls within a given interval.<br/>Labels:<br/>- _topic_ – topic name. |

## Aggregated topic partition metrics {#topics_partitions}

The following table lists aggregated partition metrics for a topic. Maximum and minimum values are calculated across all partitions of the specified topic.

| Metric name<br/>Type, units | Description<br/>Labels |
| --- | --- |
| `topic.partition.init_duration_milliseconds_max`<br/>`GAUGE`, milliseconds | Maximum partition initialization delay.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.partition.producers_count_max`<br/>`GAUGE`, count | Maximum number of sources in a partition.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.partition.storage_bytes_max`<br/>`GAUGE`, bytes | Maximum partition size in bytes.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.partition.uptime_milliseconds_min`<br/>`GAUGE`, count | Minimum partition uptime after restart.<br/>Normally during a rolling restart `topic.partition.uptime_milliseconds_min` is close to 0, after the rolling restart ends, the value of `topic.partition.uptime_milliseconds_min` should increase to infinity.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.partition.total_count`<br/>`GAUGE`, count | Total number of partitions in the topic.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.partition.alive_count`<br/>`GAUGE`, count | Number of partitions reporting their metrics.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.partition.committed_end_to_end_lag_milliseconds_max`<br/>`GAUGE`, milliseconds | Maximum (across all partitions) difference between the current time and the creation time of the last committed message.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name. |
| `topic.partition.committed_lag_messages_max`<br/>`GAUGE`, units | Maximum (across all partitions) difference between the last partition offset and the committed partition offset.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name. |
| `topic.partition.committed_read_lag_milliseconds_max`<br/>`GAUGE`, milliseconds | Maximum (across all partitions) difference between the current time and the write time of the last committed message.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name. |
| `topic.partition.end_to_end_lag_milliseconds_max`<br/>`GAUGE`, milliseconds | Difference between the current time and the minimum creation time among all messages read in the last minute across all partitions.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name. |
| `topic.partition.lag_messages_max`<br/>`GAUGE`, units | Maximum difference (across all partitions) between the last offset in the partition and the last read offset.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name. |
| `topic.partition.read.idle_milliseconds_max`<br/>`GAUGE`, milliseconds | Maximum idle time (how long the partition has not been read from) across all partitions.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name. |
| `topic.partition.read.lag_milliseconds_max`<br/>`GAUGE`, milliseconds | Difference between the current time and the minimum write time among all messages read in the last minute across all partitions.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name. |
| `topic.partition.write.lag_milliseconds_max`<br/>`GAUGE`, milliseconds | Maximum difference between the write time and the creation time among all messages written in the last minute.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.partition.write.speed_limit_bytes_per_second`<br/>`GAUGE`, bytes per second | Write quota in bytes per second per partition.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.partition.write.throttled_nanoseconds_max`<br/>`GAUGE`, nanoseconds | Maximum write throttling time (waiting on quota) across all partitions. In the limit, if `topic.partition.write.throttled_nanoseconds_max` = 10^9, it means that the entire second was spent waiting on quota.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.partition.write.bytes_per_day_max`<br/>`GAUGE`, bytes | Maximum number of bytes written in the last 24 hours across all partitions.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.partition.write.bytes_per_hour_max`<br/>`GAUGE`, bytes | Maximum number of bytes written in the last hour across all partitions.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.partition.write.bytes_per_minute_max`<br/>`GAUGE`, bytes | Maximum number of bytes written in the last minute across all partitions.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.partition.write.idle_milliseconds_max`<br/>`GAUGE`, milliseconds | Maximum partition write idle time.<br/>Labels:<br/>- _topic_ – topic name. |

## Resource pool metrics {#resource_pools}

| Metric name<br/>Type, units | Description<br/>Labels |
| --- | --- |
| `kqp.workload_manager.CpuQuotaManager.AverageLoadPercentage`<br/>`RATE`, units | Average database load, `DATABASE_LOAD_CPU_THRESHOLD` operates based on this metric. |
| `kqp.workload_manager.InFlightLimit`<br/>`GAUGE`, units | Limit on the number of concurrently running queries. |
| `kqp.workload_manager.GlobalInFly`<br/>`GAUGE`, units | Current number of concurrently running queries. Displayed only for pools with `CONCURRENT_QUERY_LIMIT` or `DATABASE_LOAD_CPU_THRESHOLD` enabled. |
| `kqp.workload_manager.QueueSizeLimit`<br/>`GAUGE`, units | Size of the queue of queries waiting to be executed. |
| `kqp.workload_manager.GlobalDelayedRequests`<br/>`GAUGE`, units | Number of queries waiting in the execution queue. Displayed only for pools with `CONCURRENT_QUERY_LIMIT` or `DATABASE_LOAD_CPU_THRESHOLD` enabled. |

## See also

- [Grafana dashboards for monitoring YDB metrics](grafana-dashboards.md)
