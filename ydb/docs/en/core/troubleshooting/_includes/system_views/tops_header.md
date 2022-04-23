## Top queries {#top-queries}

* **top_queries_by_duration_one_minute**
* **top_queries_by_duration_one_hour**
* **top_queries_by_read_bytes_one_minute**
* **top_queries_by_read_bytes_one_hour**
* **top_queries_by_cpu_time_one_minute**
* **top_queries_by_cpu_time_one_hour**

A group of system views for analyzing the flow of user queries. They let you see a time-limited query history divided into intervals. Within a single interval, the top 5 queries by a specific metric are saved. Currently, minute and hour intervals are available, and the top list can be made based on the total query execution time (the slowest), the number of bytes read from the table (the widest), and the total CPU time used (the heaviest).

Different runs of a query with the same text are deduplicated. The top list contains information about a specific run with the maximum value of the corresponding query metric within a single interval.

Fields that provide information about the used CPU time (...CPUTime) are expressed in ms.

Table structure:

| **Field** | **Type** | **Key** | **Value** |
| --- | --- | --- | --- |
| IntervalEnd | Timestamp | 0 | Closing time of a minute or hour interval |
| Rank | Uint32 | 1 | Rank of a top query |
| RequestUnits | Uint64 |  | Number of [RequestUnits](../../../concepts/serverless_and_dedicated.md#serverless-options) used |
| QueryText | Utf8 |  | Query text |
| Duration | Interval |  | Total time of query execution |
| EndTime | Timestamp |  | Query execution end time |
| Type | String |  | Query type (data, scan, or script) |
| ReadRows | Uint64 |  | Number of rows read |
| ReadBytes | Uint64 |  | Number of bytes read |
| UpdateRows | Uint64 |  | Number of rows updated |
| UpdateBytes | Uint64 |  | Number of bytes updated |
| DeleteRows | Uint64 |  | Number of rows deleted |
| DeleteBytes | Uint64 |  | Number of bytes deleted |
| Partitions | Uint64 |  | Number of table partitions used during query execution |
| UserSID | String |  | User security ID |
| ParametersSize | Uint64 |  | Size of query parameters in bytes |
| CompileDuration | Interval |  | Query compile duration |
| FromQueryCache | Bool |  | Shows whether the cache of prepared queries was used |
| CPUTime | Uint64 |  | Total CPU time used to execute the query (ms) |
| ShardCount | Uint64 |  | Number of shards used during query execution |
| SumShardCPUTime | Uint64 |  | Total CPU time used in shards |
| MinShardCPUTime | Uint64 |  | Minimum CPU time used in shards |
| MaxShardCPUTime | Uint64 |  | Maximum CPU time used in shards |
| ComputeNodesCount | Uint64 |  | Number of compute nodes used during query execution |
| SumComputeCPUTime | Uint64 |  | Total CPU time used in compute nodes |
| MinComputeCPUTime | Uint64 |  | Minimum CPU time used in compute nodes |
| MaxComputeCPUTime | Uint64 |  | Maximum CPU time used in compute nodes |
| CompileCPUTime | Uint64 |  | CPU time used to compile a query |
| ProcessCPUTime | Uint64 |  | CPU time used for overall query handling |

Restrictions:

* Query text limit is 4 KB.
* Tables with minute intervals contain the history for the last 6 hours.
* Tables with hourly intervals contain the history for the last 2 weeks.

