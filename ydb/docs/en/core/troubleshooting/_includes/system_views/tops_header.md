## Top queries {#top-queries}

The following system views store data for analyzing the flow of user queries:

* `top_queries_by_duration_one_minute`: Data is split into one-minute intervals, contains Top 5 queries with the maximum total execution time for the last 6 hours.
* `top_queries_by_duration_one_hour`: Data is split into one-hour intervals, contains Top 5 queries with the maximum total execution time for the last 2 weeks.
* `top_queries_by_read_bytes_one_minute`: Data is split into one-minute intervals, contains Top 5 queries with the maximum number of bytes read from the table for the last 6 hours.
* `top_queries_by_read_bytes_one_hour`: Data is split into one-hour intervals, contains Top 5 queries with the maximum number of bytes read from the table for the last 2 weeks.
* `top_queries_by_cpu_time_one_minute`: Data is split into one-minute intervals, contains Top 5 queries with the maximum CPU time used for the last 6 hours.
* ` top_queries_by_cpu_time_one_hour`: Data is split into one-hour intervals, contains Top 5 queries with the maximum CPU time used for the last 2 weeks.

Different runs of a query with the same text are deduplicated. The top list contains information about a specific run with the maximum value of the corresponding query metric within a single interval.

Fields that provide information about the used CPU time (...`CPUTime`) are expressed in microseconds.

Query text limit is 4 KB.

All tables have the same set of fields:

| Field | Description |
--- | ---
| `IntervalEnd` | The end of a one-minute or one-hour interval.<br>Type: `Timestamp`.<br>Key: `0`. |
| `Rank` | Rank of a top query.<br>Type: `Uint32`.<br>Key: `1`. |
| `QueryText` | Query text.<br>Type: `Utf8`. |
| `Duration` | Total query execution time.<br>Type: `Interval`. |
| `EndTime` | Query execution end time. <br>Type: `Timestamp`. |
| `Type` | Query type (data, scan, or script).<br>Type: `String`. |
| `ReadRows` | Number of rows read.<br>Type: `Uint64`. |
| `ReadBytes` | Number of bytes read.<br>Type: `Uint64`. |
| `UpdateRows` | Number of rows written.<br>Type: `Uint64`. |
| `UpdateBytes` | Number of bytes written.<br>Type: `Uint64`. |
| `DeleteRows` | Number of rows deleted.<br>Type: `Uint64`. |
| `DeleteBytes` | Number of bytes deleted.<br>Type: `Uint64`. |
| `Partitions` | Number of table partitions used during query execution.<br>Type: `Uint64`. |
| `UserSID` | User Security ID.<br>Type: `String`. |
| `ParametersSize` | Size of query parameters in bytes.<br>Type: `Uint64`. |
| `CompileDuration` | Duration of query compilation.<br>Type: `Interval`. |
| `FromQueryCache` | Shows whether the cache of prepared queries was used.<br>Type: `Bool`. |
| `CPUTime` | Total CPU time used to execute the query (microseconds).<br>Type: `Uint64`. |
| `ShardCount` | Number of shards used during query execution.<br>Type: `Uint64`. |
| `SumShardCPUTime` | Total CPU time used in shards.<br>Type: `Uint64`. |
| `MinShardCPUTime` | Minimum CPU time used in shards.<br>Type: `Uint64`. |
| `MaxShardCPUTime` | Maximum CPU time used in shards.<br>Type: `Uint64`. |
| `ComputeNodesCount` | Number of compute nodes used during query execution.<br>Type: `Uint64`. |
| `SumComputeCPUTime` | Total CPU time used in compute nodes.<br>Type: `Uint64`. |
| `MinComputeCPUTime` | Minimum CPU time used in compute nodes.<br>Type: `Uint64`. |
| `MaxComputeCPUTime` | Maximum CPU time used in compute nodes.<br>Type: `Uint64`. |
| `CompileCPUTime` | CPU time used to compile a query.<br>Type: `Uint64`. |
| `ProcessCPUTime` | CPU time used for overall query handling.<br>Type: `Uint64`. |
