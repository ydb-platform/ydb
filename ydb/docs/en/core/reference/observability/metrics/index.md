# Metrics reference

## Resource usage metrics {#resources}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| ----- | ----- |
| `resources.storage.used_bytes`<br/>`IGAUGE`, bytes | The size of user and service data stored in distributed network storage. `resources.storage.used_bytes` = `resources.storage.table.used_bytes` + `resources.storage.topic.used_bytes`. |
| `resources.storage.table.used_bytes`<br/>`IGAUGE`, bytes | The size of user and service data stored by tables in distributed network storage. Service data includes the data of the primary, [secondary indexes](../../../concepts/glossary.md#secondary-index) and [vector indexes](../../../concepts/glossary.md#vector-index). |
| `resources.storage.topic.used_bytes`<br/>`IGAUGE`, bytes | The size of storage used by topics. This metric sums the `topic.storage_bytes` values of all topics. |
| `resources.storage.limit_bytes`<br/>`IGAUGE`, bytes | A limit on the size of user and service data that a database can store in distributed network storage. |

## API metrics {#api}

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

<<<<<<< HEAD
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
=======
| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| --- | --- |
| `topic.producers_count`<br/>`GAUGE`, count | Number of unique topic [sources](../../../concepts/datamodel/topic#producer-id).<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.storage_bytes`<br/>`GAUGE`, bytes | Topic size in bytes.<br/>Labels:<br/>- _topic_ – topic name. |
| `topic.read.bytes`<br/>`RATE`, bytes | Number of bytes read from the topic.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – reader name. |
| `topic.read.messages`<br/>`RATE`, count | Number of messages read from the topic.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – reader name. |
| `topic.read.lag_messages`<br/>`GAUGE`, count | Total number of messages not yet read by the given reader across the topic. The metric serves as an indicator of reader lag. An increase in the value means the reader is falling behind the message flow — for example, due to a reader stop, partition rebalancing, or a write load spike.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – reader name. |
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
| `topic.partition.committed_read_lag_milliseconds_max`<br/>`GAUGE`, milliseconds | The maximum (across all partitions) difference between the current time and the time of the oldest uncommitted message. A value greater than zero indicates that the topic contains at least one message that has been written and may already have been consumed, but has not yet been committed.<br/>Labels:<br/>- _topic_ – topic name.<br/>- _consumer_ – consumer name|
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
>>>>>>> 8af4af1994b (Expand topic.read.lag_messages metric description (#52520))
