## Query details {#query-metrics}

* **query_metrics_one_minute**

Detailed information about queries, broken down by minute. Each table row contains information about a set of queries with identical text that were made during one minute. The table fields provide the minimum, maximum, and total values for each query metric tracked. Within the interval, queries are sorted in descending order of the total CPU time used.

Table structure:

| **Field** | **Type** | **Key** | **Value** |
| --- | --- | --- | --- |
| IntervalEnd | Timestamp | 0 | Closing time of a minute interval |
| Rank | Uint32 | 1 | Query rank per interval (by the SumCPUTime field) |
| QueryText | Utf8 |  | Query text |
| Count | Uint64 |  | Number of query runs |
| SumDuration | Interval |  | Total query duration |
| MinDuration | Interval |  | Minimum query duration |
| MaxDuration | Interval |  | Maximum query duration |
| SumCPUTime | Uint64 |  | Total CPU time used |
| MinCPUTime | Uint64 |  | Minimum CPU time used |
| MaxCPUTime | Uint64 |  | Maximum CPU time used |
| SumReadRows | Uint64 |  | Total number of rows read |
| MinReadRows | Uint64 |  | Minimum number of rows read |
| MaxReadRows | Uint64 |  | Maximum number of rows read |
| SumReadBytes | Uint64 |  | Total number of bytes read |
| MinReadBytes | Uint64 |  | Minimum number of bytes read |
| MaxReadBytes | Uint64 |  | Maximum number of bytes read |
| SumUpdateRows | Uint64 |  | Total number of rows updated |
| MinUpdateRows | Uint64 |  | Minimum number of rows updated |
| MaxUpdateRows | Uint64 |  | Maximum number of rows updated |
| SumUpdateBytes | Uint64 |  | Total number of bytes updated |
| MinUpdateBytes | Uint64 |  | Minimum number of bytes updated |
| MaxUpdateBytes | Uint64 |  | Maximum number of bytes updated |
| SumDeleteRows | Uint64 |  | Total number of rows deleted |
| MinDeleteRows | Uint64 |  | Minimum number of rows deleted |
| MaxDeleteRows | Uint64 |  | Maximum number of rows deleted |
| SumRequestUnits | Uint64 |  | Total number of [RequestUnits](../../../concepts/serverless_and_dedicated.md#serverless-options) used |
| MinRequestUnits | Uint64 |  | Minimum number of [RequestUnits](../../../concepts/serverless_and_dedicated.md#serverless-options) used |
| MaxRequestUnits | Uint64 |  | Maximum number of [RequestUnits](../../../concepts/serverless_and_dedicated.md#serverless-options) used |

Restrictions:

* Query text limit is 4 KB.
* The table contains the history for the last 6 hours.
* Within the interval, information is provided for no more than 256 different queries.
* Statistics may be incomplete if the database is under heavy load.

