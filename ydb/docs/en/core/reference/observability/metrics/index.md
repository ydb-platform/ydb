# Metrics reference

## Resource usage metrics {#resources}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| ----- | ----- |
| `resources.storage.used_bytes`<br/>`IGAUGE`, bytes | The size of user and service data stored in distributed network storage. `resources.storage.used_bytes` = `resources.storage.table.used_bytes` + `resources.storage.topic.used_bytes`. |
| `resources.storage.table.used_bytes`<br/>`IGAUGE`, bytes | The size of user and service data stored by tables in distributed network storage. Service data includes the data of the primary, [secondary indexes](../../../concepts/glossary.md#secondary-index), [vector indexes](../../../concepts/glossary.md#vector-index), and [fulltext indexes](../../../concepts/glossary.md#fulltext-index). |
| `resources.storage.topic.used_bytes`<br/>`IGAUGE`, bytes | The size of storage used by topics. This metric sums the `topic.storage_bytes` values of all topics. |
| `resources.storage.limit_bytes`<br/>`IGAUGE`, bytes | A limit on the size of user and service data that a database can store in distributed network storage. |

## GRPC API metrics {#api}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| ----- | ----- |
| `api.grpc.request.bytes`<br/>`RATE`, bytes | The size of queries received by the database in a certain period of time.<br/>Labels:<br/>- _api_service_: The name of the gRPC API service, such as `table` or `data_streams`.<br/>- _method_: The name of a gRPC API service method, such as `ExecuteDataQuery` (for `table` service) or `PutRecord`, `GetRecords` (for `data_stream` service). |
| `api.grpc.request.dropped_count`<br/>`RATE`, pieces | The number of requests dropped at the transport (gRPC) layer due to an error.<br/>Labels:<br/>- _api_service_: The name of the gRPC API service, such as `table`.<br/>- _method_: The name of a gRPC API service method, such as `ExecuteDataQuery`. |
| `api.grpc.request.inflight_count`<br/>`IGAUGE`, pieces | The number of requests that a database is simultaneously handling in a certain period of time.<br/>Labels:<br/>- _api_service_: The name of the gRPC API service, such as `table`.<br/>- _method_: The name of a gRPC API service method, such as `ExecuteDataQuery`. |
| `api.grpc.request.inflight_bytes`<br/>`IGAUGE`, bytes | The size of requests that a database is simultaneously handling in a certain period of time.<br/>Labels:<br/>- _api_service_: The name of the gRPC API service, such as `table`.<br/>- _method_: The name of a gRPC API service method, such as `ExecuteDataQuery`. |
| `api.grpc.response.bytes`<br/>`RATE`, bytes | The size of responses sent by the database in a certain period of time.<br/>Labels:<br/>- _api_service_: The name of the gRPC API service, such as `table`.<br/>- _method_: The name of a gRPC API service method, such as `ExecuteDataQuery`. |
| `api.grpc.response.count`<br/>`RATE`, pieces | The number of responses sent by the database in a certain period of time.<br/>Labels:<br/>- _api_service_: The name of the gRPC API service, such as `table`.<br/>- _method_: The name of a gRPC API service method, such as `ExecuteDataQuery`.<br/>- _status_ is the request execution status. See a more detailed description of statuses under [Error Handling](../../../reference/ydb-sdk/error_handling.md). |
| `api.grpc.response.dropped_count`<br/>`RATE`, pieces | The number of responses dropped at the transport (gRPC) layer due to an error.<br/>Labels:<br/>- _api_service_: The name of the gRPC API service, such as `table`.<br/>- _method_: The name of a gRPC API service method, such as `ExecuteDataQuery`. |
| `api.grpc.response.issues`<br/>`RATE`, pieces | The number of errors of a certain type arising in the execution of a request over a certain period of time.<br/>Tags:<br/>- _issue_type_ is the error type wth the only value being `optimistic_locks_invalidation`. For more on lock invalidation, review [Transactions and requests to {{ ydb-short-name }}](../../../concepts/transactions.md). |

## GRPC API metrics fot topics {#grpc_api_topics}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| ----- | ----- |
| `grpc.topic.stream_read.commits`<br/>`RATE`, | Commit number of method `Ydb::TopicService::StreamRead`.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name |
| `grpc.topic.stream_read.bytes`<br/>`RATE`, pieces | Number of bytes read by the `Ydb::TopicService::StreamRead` method.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name |
| `grpc.topic.stream_read.messages`<br/>`RATE`, pieces | Number of messages read by the method `Ydb::TopicService::StreamRead`.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name |
| `grpc.topic.stream_read.partition_session.errors`<br/>`RATE`, pieces | Number of errors when working with the partition.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name |
| `grpc.topic.stream_read.partition_session.started`<br/>`RATE`, pieces | The number of sessions launched per unit of time.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name |
| `grpc.topic.stream_read.partition_session.stopped`<br/>`RATE`, pieces | Number of sessions stopped per unit of time.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name |
| `grpc.topic.stream_read.partition_session.starting_count`<br/>`RATE`, pieces | The number of sessions being launched (it means, the client received a command to start a session, but the client has not yet launched the session).<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name |
| `grpc.topic.stream_read.partition_session.stopping_count`<br/>`RATE`, pieces | Number of sessions stopped.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name |
| `grpc.topic.stream_read.partition_session.count`<br/>`RATE`, pieces | Number of partition_session.<br/>Label:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name |
| `grpc.topic.stream_write.bytes`<br/>`RATE`, bytes | Number of bytes writing by `Ydb::TopicService::StreamWrite`.<br/>Labels:<br/>- _topic_ – topic name |
| `grpc.topic.stream_write.uncommitted_bytes`<br/>`RATE`, bytes | The number of bytes written by the `Ydb::TopicService::StreamWrite` method within transactions that have not yet been committed.<br/>Label:<br/>- _topic_ – topic name |
| `grpc.topic.stream_write.errors`<br/>`RATE`, pieces | Number of errors when calling the `Ydb::TopicService::StreamWrite` method.<br/>Labels:<br/>- _topic_ – topic name |
| `grpc.topic.stream_write.messages`<br/>`RATE`, pieces | Number of messages written by method `Ydb::TopicService::StreamWrite`.<br/>Label:<br/>- _topic_ – topic name |
| `grpc.topic.stream_write.uncommitted_messages`<br/>`RATE`, pieces | Number of messages written by method `Ydb::TopicService::StreamWrite` within transactions that have not yet been committed.<br/>Label:<br/>- _topic_ – topic name |
| `grpc.topic.stream_write.partition_throttled_milliseconds`<br/>`HIST_RATE`, pieces | Histogram counter. Intervals are specified in milliseconds. Shows the number of messages waiting at the quota.<br/>Label:<br/>- _topic_ – topic name |
| `grpc.topic.stream_write.sessions_active_count`<br/>`GAUGE`, pieces | Number of open recording sessions.<br/>Метки:<br/>- _topic_ – topic name |
| `grpc.topic.stream_write.sessions_created`<br/>`RATE`, pieces | Number of recording sessions created.<br/>Метки:<br/>- _topic_ – topic name |

## HTTP API Metrics {#http_api}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| ----- | ----- |
| `api.http.data_streams.request.count`<br/>`RATE`, pieces  | Number of HTTP requests.<br/>Labels:<br/>- _method_ – name of the HTTP API service method, for example `PutRecord` , `GetRecords`.<br/>- _topic_ – topic name |
| `api.http.data_streams.request.bytes`<br/>`RATE`, bytes  | Total size of HTTP requests.<br/>Labels:<br/>- _method_ – name of the HTTP API service method, in this case only `PutRecord`.<br/>- _topic_ – topic name |
| `api.http.data_streams.response.count`<br/>`RATE`, pieces  | Number of responses via HTTP protocol.<br/>Label:<br/>- _method_ – name of the HTTP API service method, for example `PutRecord` , `GetRecords`.<br/>- _topic_ – topic name.<br/>- _code_ – HTTP response code |
| `api.http.data_streams.response.bytes`<br/>`RATE`, bytes  | Total size of HTTP responses.<br/>Label:<br/>- _method_ – name of the HTTP API service method, in this case only `GetRecords`.<br/>- _topic_ – topic name |
| `api.http.data_streams.response.duration_milliseconds`<br/>`HIST_RATE`, pieces  | Histogram counter. Intervals are specified in milliseconds. Shows the number of responses whose execution time falls within a certain interval.<br/>Label:<br/>- _method_ – name of the HTTP API service method.<br/>- _topic_ – topic name |
| `api.http.data_streams.get_records.messages`<br/>`RATE`, pieces | Number of messages written by the method `GetRecords`.<br/>Labels:<br/>- _topic_ – topic name |
| `api.http.data_streams.put_record.messages`<br/>`RATE`, pieces | Number of messages written by the method `PutRecord` (always =1).<br/>Label:<br/>- _topic_ – topic name |
| `api.http.data_streams.put_records.failed_messages`<br/>`RATE`, pieces | The number of messages sent by the `PutRecords` method that were not recorded.<br/>Метки:<br/>- _topic_ – topic name |
| `api.http.data_streams.put_records.successful_messages`<br/>`RATE`, pieces | The number of messages sent by the `PutRecords` method that were successfully written.<br/>Метки:<br/>- _topic_ – topic name |
| `api.http.data_streams.put_records.total_messages`<br/>`RATE`, pieces | Number of messages sent using the `PutRecords` method.<br/>Label:<br/>- _topic_ – topic name |

## Kafka API metrics {#kafka_api}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| ----- | ----- |
| `api.kafka.request.count`<br/>`RATE`, pieces  | The number of requests via the Kafka protocol per unit of time.<br/>Label:<br/>- _method_ – name of the Kafka API service method, for example `PRODUCE`, `SASL_HANDSHAKE` |
| `api.kafka.request.bytes`<br/>`RATE`, bytes | Total size of Kafka requests per unit of time.<br/>Label:<br/>- _method_ – name of the Kafka API service method, for example `PRODUCE`, `SASL_HANDSHAKE` |
| `api.kafka.response.count`<br/>`RATE`, pieces  | The number of responses via the Kafka protocol per unit of time.<br/>Label:<br/>- _method_ – name of the Kafka API service method, for example `PRODUCE`, `SASL_HANDSHAKE`.<br/>- _error_code_ – Kafka response code |
| `api.kafka.response.bytes`<br/>`RATE`, bytes | The total size of responses via the Kafka protocol per unit of time.<br/>Label:<br/>- _method_ – name of the Kafka API service method, for example `PRODUCE`, `SASL_HANDSHAKE` |
| `api.kafka.response.duration_milliseconds`<br/>`HIST_RATE`, pieces | Histogram counter. Defines a set of intervals in milliseconds and for each of them shows the number of requests with execution time falling within this interval.<br/>Label:<br/>- _method_ – name of the Kafka API service method |
| `api.kafka.produce.failed_messages`<br/>`RATE`, pieces | The number of messages per unit of time sent by the `PRODUCE` method that were not recorded.<br/>Label:<br/>- _topic_ – topic name |
| `api.kafka.produce.successful_messages`<br/>`RATE`, pieces | The number of messages per unit of time sent by the `PRODUCE` method that were successfully recorded.<br/>Метки:<br/>- _topic_ – topic name |
| `api.kafka.produce.total_messages`<br/>`RATE`, pieces | Number of messages per unit of time sent by the `PRODUCE` method.<br/>Label:<br/>- _topic_ – topic name |

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

## Row-oriented table partition metrics {#datashards}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| ----- | ----- |
| `table.datashard.row_count`<br/>`GAUGE`, pieces | The number of rows in all row-oriented tables in the database. |
| `table.datashard.size_bytes`<br/>`GAUGE`, bytes | The size of data in all row-oriented tables in the database. |
| `table.datashard.used_core_percents`<br/>`HIST_GAUGE`, % | Histogram counter. The intervals are set as a percentage. Shows the number of row-oriented table partitions using computing resources in the ratio that falls within a certain interval. |
| `table.datashard.read.rows`<br/>`RATE`, pieces | The number of rows that are read by all partitions of all row-oriented tables in the database in a certain period of time. |
| `table.datashard.read.bytes`<br/>`RATE`, bytes | The size of data that is read by all partitions of all row-oriented tables in the database in a certain period of time. |
| `table.datashard.write.rows`<br/>`RATE`, pieces | The number of rows that are written by all partitions of all row-oriented tables in the database in a certain period of time. |
| `table.datashard.write.bytes`<br/>`RATE`, bytes | The size of data that is written by all partitions of all row-oriented tables in the database in a certain period of time. |
| `table.datashard.scan.rows`<br/>`RATE`, pieces | The number of rows that are read through `StreamExecuteScanQuery` or `StreamReadTable` gRPC API calls by all partitions of all row-oriented tables in the database in a certain period of time. |
| `table.datashard.scan.bytes`<br/>`RATE`, bytes | The size of data that is read through `StreamExecuteScanQuery` or `StreamReadTable` gRPC API calls by all partitions of all row-oriented tables in the database in a certain period of time. |
| `table.datashard.bulk_upsert.rows`<br/>`RATE`, pieces | The number of rows that are added through a `BulkUpsert` gRPC API call to all partitions of all row-oriented tables in the database in a certain period of time. |
| `table.datashard.bulk_upsert.bytes`<br/>`RATE`, bytes | The size of data that is added through a `BulkUpsert` gRPC API call to all partitions of all row-oriented tables in the database in a certain period of time. |
| `table.datashard.erase.rows`<br/>`RATE`, pieces | The number of rows deleted from row-oriented tables in the database in a certain period of time. |
| `table.datashard.erase.bytes`<br/>`RATE`, bytes | The size of data deleted from row-oriented tables in the database in a certain period of time. |
| `table.datashard.cache_hit.bytes`<br/>`RATE`, bytes | The total amount of data successfully retrieved from memory (cache), indicating efficient cache utilization in serving frequently accessed data without accessing distributed storage. |
| `table.datashard.cache_miss.bytes`<br/>`RATE`, bytes | The total amount of data that was requested but not found in memory (cache) and was read from distributed storage, highlighting potential areas for cache optimization. |

## Column-oriented table partition metrics {#columnshards}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| ----- | ----- |
| `table.columnshard.write.rows`<br/>`RATE`, pieces | The number of rows that are written by all partitions of all column-oriented tables in the database in a certain period of time. |
| `table.columnshard.write.bytes`<br/>`RATE`, bytes | The size of data that is written by all partitions of all column-oriented tables in the database in a certain period of time. |
| `table.columnshard.scan.rows`<br/>`RATE`, pieces | The number of rows that are read through `StreamExecuteScanQuery` or `StreamReadTable` gRPC API calls by all partitions of all column-oriented tables in the database in a certain period of time. |
| `table.columnshard.scan.bytes`<br/>`RATE`, bytes | The size of data that is read through `StreamExecuteScanQuery` or `StreamReadTable` gRPC API calls by all partitions of all column-oriented tables in the database in a certain period of time. |
| `table.columnshard.bulk_upsert.rows`<br/>`RATE`, pieces | The number of rows that are added through a `BulkUpsert` gRPC API call to all partitions of all column-oriented tables in the database in a certain period of time. |
| `table.columnshard.bulk_upsert.bytes`<br/>`RATE`, bytes | The size of data that is added through a `BulkUpsert` gRPC API call to all partitions of all column-oriented tables in the database in a certain period of time. |

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
| `topic.write.uncompressed_bytes`<br/>`RATE`, bytes | The size of uncompressed written data.<br/>Метки:<br/>- _topic_ – the name of the topic. |
| `topic.write.messages`<br/>`RATE`, pieces | The number of written messages.<br/>Labels:<br/>- _topic_ – the name of the topic. |
| `topic.write.uncommitted_messages`<br/>`RATE`, pieces | The number of messages written as part of ongoing transactions.<br/>Labels:<br/>- _topic_ — the name of the topic. |
| `topic.write.message_size_bytes`<br/>`HIST_RATE`, pieces | A histogram counter. The intervals are specified in bytes. It shows the number of messages which size falls within the boundaries of the interval.<br/>Labels:<br/>- _topic_ – the name of the topic. |
| `topic.write.lag_milliseconds`<br/>`HIST_RATE`, pieces | A histogram counter. The intervals are specified in milliseconds. It shows the number of messages where the difference between the write time and the message creation time falls within the specified interval.<br/>Labels:<br/>- _topic_ – the name of the topic. |


## Aggregated metrics of topic partitions {#topics_partitions}

The following table shows aggregated partition metrics for the topic. The maximum and minimum values ​​are calculated for all partitions of a given topic.

| Metric name<br/>Type<br/>units of measurement | Description<br/>Labels |
| ----- | ----- |
| `topic.partition.init_duration_milliseconds_max`<br/>`GAUGE`, milliseconds | Maximum partition initialization delay.<br/>Метки:<br/>- _topic_ – topic name |
| `topic.partition.producers_count_max`<br/>`GAUGE`, pieces | The maximum number of sources in the partition.<br/>Метки:<br/>- _topic_ – topic name |
| `topic.partition.storage_bytes_max`<br/>`GAUGE`, bytes | Maximum partition size in bytes.<br/>Label:<br/>- _topic_ – topic name |
| `topic.partition.uptime_milliseconds_min`<br/>`GAUGE`, pieces | Minimum partition operating time after restart.<br/>Normally during a rolling restart of `topic.partition.uptime_milliseconds_min` is close to 0, after the end of the rolling restart the value of `topic.partition.uptime_milliseconds_min` should increase to infinity.<br/>Label:<br/>- _topic_ – topic name |
| `topic.partition.total_count`<br/>`GAUGE`, pieces | Total number of partitions in the topic.<br/>Label:<br/>- _topic_ – topic name |
| `topic.partition.alive_count`<br/>`GAUGE`, pieces | The number of partitions sending their metrics.<br/>Label:<br/>- _topic_ – topic name |
| `topic.partition.committed_end_to_end_lag_milliseconds_max`<br/>`GAUGE`, milliseconds | The maximum (across all partitions) difference between the current time and the time the last downloaded message was created.<br/>Label:<br/>- _topic_ – topic name.<br/>- _consumer_ – name of the consumer |
| `topic.partition.committed_lag_messages_max`<br/>`GAUGE`, pieces | The maximum (across all partitions) difference between the last partition offset and the recorded partition offset.<br/>Метки:<br/>- _topic_ – topic name.<br/>- _consumer_ – name of the consumer |
| `topic.partition.committed_read_lag_milliseconds_max`<br/>`GAUGE`, milliseconds | The maximum (across all partitions) difference between the current time and the recording time of the last recorded message.<br/>Label:<br/>- _topic_ – topic name.<br/>- _consumer_ – name of the consumer |
| `topic.partition.end_to_end_lag_milliseconds_max`<br/>`GAUGE`, milliseconds | The difference between the current time and the minimum creation time among all messages read in the last minute in all partitions.<br/>Label:<br/>- _topic_ – topic name.<br/>- _consumer_ – name of the consumer |
| `topic.partition.lag_messages_max`<br/>`GAUGE`, pieces | The maximum difference (across all partitions) of the last offset in the partition and the last subtracted offset.<br/>Label:<br/>- _topic_ – topic name.<br/>- _consumer_ – name of the consumer |
| `topic.partition.read.lag_milliseconds_max`<br/>`GAUGE`, milliseconds | The difference between the current time and the minimum recording time among all messages read in the last minute in all partitions.<br/>Label:<br/>- _topic_ – topic name.<br/>- _consumer_ – name of the consumer |
| `topic.partition.read.idle_milliseconds_max`<br/>`GAUGE`, milliseconds | Maximum idle time (how long the partition was not read) for all partitions.<br/>Label:<br/>- _topic_ – topic name.<br/>- _consumer_ – name of the consumer |
| `topic.partition.read.lag_milliseconds_max`<br/>`GAUGE`, milliseconds | The maximum difference between the recording time and the creation time among all messages read in the last minute.<br/>Label:<br/>- _topic_ – topic name.<br/>- _consumer_ – name of the consumer |
| `topic.partition.write.lag_milliseconds_max`<br/>`GAUGE`, milliseconds | The maximum difference between the recording time and the creation time among all messages recorded in the last minute.<br/>Label:<br/>- _topic_ – topic name |
| `topic.partition.write.speed_limit_bytes_per_second`<br/>`GAUGE`, bytes per second | Write quota in bytes per second per partition.<br/>Label:<br/>- _topic_ – topic name |
| `topic.partition.write.throttled_nanoseconds_max`<br/>`GAUGE`, nanoseconds | Maximum write throttling time (waiting on quota) for all partitions. In the limit, if `topic.partition.write.throttled_nanoseconds_max` = 10^9, then this means that the entire second was waited on the quota<br/>Label:<br/>- _topic_ – topic name |
| `topic.partition.write.bytes_per_day_max`<br/>`GAUGE`, bytes | The maximum number of bytes written over the last 24 hours for all partitions.<br/>Label:<br/>- _topic_ – topic name |
| `topic.partition.write.bytes_per_hour_max`<br/>`GAUGE`, bytes | The maximum number of bytes written in the last hour, across all partitions.<br/>Label:<br/>- _topic_ – topic name |
| `topic.partition.write.bytes_per_minute_max`<br/>`GAUGE`, bytes | The maximum number of bytes written in the last minute, across all partitions.<br/>Label:<br/>- _topic_ – topic name |
| `topic.partition.write.idle_milliseconds_max`<br/>`GAUGE`, milliseconds | Maximum time the partition is idle for recording.br/>Label:<br/>- _topic_ – topic name |

## Resource pool metrics {#resource_pools}

| Metric name<br/>Type, units of measurement | Description<br/>Tags |
| ----- | ----- |
| `kqp.workload_manager.CpuQuotaManager.AverageLoadPercentage`<br/>`RATE`, pieces | Average database load, the `DATABASE_LOAD_CPU_THRESHOLD` works based on this metric. |
| `kqp.workload_manager.InFlightLimit`<br/>`GAUGE`, pieces | Limit on the number of simultaneously running requests. |
| `kqp.workload_manager.GlobalInFly`<br/>`GAUGE`, pieces | The current number of simultaneously running requests. Displayed only for pools with `CONCURRENT_QUERY_LIMIT` or `DATABASE_LOAD_CPU_THRESHOLD` enabled |
| `kqp.workload_manager.QueueSizeLimit`<br/>`GAUGE`, pieces | Queue size of pending requests. |
| `kqp.workload_manager.GlobalDelayedRequests`<br/>`GAUGE`, pieces | The number of requests waiting in the execution queue. Only visible for pools with `CONCURRENT_QUERY_LIMIT` or `DATABASE_LOAD_CPU_THRESHOLD` enabled . |

