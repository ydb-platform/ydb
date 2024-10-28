# Metrics reference

## Resource usage metrics {#resources}

| Metric name<br/>Type, units of measurement | Description<br/>Labels |
| ----- | ----- |
| `resources.storage.used_bytes`<br/>`IGAUGE`, bytes | The size of user and service data stored in distributed network storage. Housekeeping data include the data of the primary and [secondary indexes](../../../concepts/secondary_indexes.md). |
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
