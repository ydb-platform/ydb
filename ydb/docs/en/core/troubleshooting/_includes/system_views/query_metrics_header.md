## Query details {#query-metrics}

The following system view stores detailed information about queries:

* `query_metrics_one_minute`: Data is split into one-minute intervals, contains up to 256 queries for the last 6 hours.

Each table row contains information about a set of queries with identical text that were made during one minute. The table fields provide the minimum, maximum, and total values for each query metric tracked. Within the interval, queries are sorted in descending order of the total CPU time used.

Restrictions:

* Query text limit is 4 KB.
* Statistics may be incomplete if the database is under heavy load.

Table structure:

| Field | Description |
---|---
| `IntervalEnd` | The end of a one-minute interval.<br>Type: `Timestamp`.<br>Key: `0`. |
| `Rank` | Query rank within an interval (by the SumCPUTime field).<br>Type: `Uint32`.<br>Key: `1`. |
| `QueryText` | Query text.<br>Type: `Utf8`. |
| `Count` | Number of query runs.<br>Type: `Uint64`. |
| `SumDuration` | Total duration of queries.<br>Type: `Interval`. |
| `Count` | Number of query runs.<br>Type: `Uint64`. |
| `SumDuration` | Total duration of queries.<br>Type: `Interval`. |
| `MinDuration` | Minimum query duration.<br>Type: `Interval`. |
| `MaxDuration` | Maximum query duration.<br>Type: `Interval`. |
| `SumCPUTime` | Total CPU time used.<br>Type: `Uint64`. |
| `MinCPUTime` | Minimum CPU time used.<br>Type: `Uint64`. |
| `MaxCPUTime` | Maximum CPU time used.<br>Type: `Uint64`. |
| `SumReadRows` | Total number of rows read.<br>Type: `Uint64`. |
| `MinReadRows` | Minimum number of rows read.<br>Type: `Uint64`. |
| `MaxReadRows` | Maximum number of rows read.<br>Type: `Uint64`. |
| `SumReadBytes` | Total number of bytes read.<br>Type: `Uint64`. |
| `MinReadBytes` | Minimum number of bytes read.<br>Type: `Uint64`. |
| `MaxReadBytes` | Maximum number of bytes read.<br>Type: `Uint64`. |
| `SumUpdateRows` | Total number of rows written.<br>Type: `Uint64`. |
| `MinUpdateRows` | Minimum number of rows written.<br>Type: `Uint64`. |
| `MaxUpdateRows` | Maximum number of rows written.<br>Type: `Uint64`. |
| `SumUpdateBytes` | Total number of bytes written.<br>Type: `Uint64`. |
| `MinUpdateBytes` | Minimum number of bytes written.<br>Type: `Uint64`. |
| `MaxUpdateBytes` | Maximum number of bytes written.<br>Type: `Uint64`. |
| `SumDeleteRows` | Total number of rows deleted.<br>Type: `Uint64`. |
| `MinDeleteRows` | Minimum number of rows deleted.<br>Type: `Uint64`. |
| `MaxDeleteRows` | Maximum number of rows deleted.<br>Type: `Uint64`. |
