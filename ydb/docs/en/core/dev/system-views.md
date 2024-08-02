# Database system views

You can make queries to special service tables (system views) to monitor the DB status. These tables are accessible from the root of the database tree and use the `.sys` system path prefix.

You can find the corresponding table's primary key field index in the descriptions of available fields below.

DB system views contain:

* [Details of individual DB table partitions](#partitions).
* [Top queries by certain characteristics](#top-queries).
* [Query details](#query-metrics).
* [History of overloaded partitions](#top-overload-partitions).

{% note info %}

Loads caused by accessing system views are more analytical in nature. Making frequent queries to them in large DBs will consume a lot of system resources. The recommended load is no more than 1-2 RPS.

{% endnote %}

## Partitions {#partitions}

The following system view stores detailed information about individual [partitions](../concepts/datamodel/table.md#partitioning) of all DB tables:

* `partition_stats`: Contains information about instant metrics and cumulative operation counters. Instant metrics are, for example, CPU load or count of in-flight [transactions](../concepts/transactions.md). Cumulative counters, for example, count the total number of rows read.

The system view is designed to detect various irregularities in the load on a table partition or show the size of table partition data.

Cumulative fields (`RowReads`, `RowUpdates`, and so on) store the accumulated values since the last start of the tablet serving the partition.

Table structure:

| Field | Description |
--- | ---
| `OwnerId` | ID of the SchemeShard serving the table.<br/>Type: `Uint64`.<br/>Key: `0`. |
| `PathId` | ID of the SchemeShard path.<br/>Type: `Uint64`.<br/>Key: `1`. |
| `PartIdx` | Partition sequence number.<br/>Type: `Uint64`.<br/>Key: `2`. |
| `DataSize` | Approximate partition size in bytes.<br/>Type: `Uint64`. |
| `RowCount` | Approximate number of rows.<br/>Type: `Uint64`. |
| `IndexSize` | Partition index size in a tablet.<br/>Type: `Uint64`. |
| `CPUCores` | Double Instant value of load per partition (CPU share) |
| `TabletId` | ID of the tablet serving the partition.<br/>Type: `Uint64`. |
| `Path` | Full path to the table.<br/>Type: `Utf8`. |
| `NodeId` | ID of the node that the partition is being served on.<br/>Type: `Uint32`. |
| `StartTime` | Last time when the tablet serving the partition was started.<br/>Type: `Timestamp`. |
| `AccessTime` | Last time when data from the partition was read.<br/>Type: `Timestamp`. |
| `UpdateTime` | Last time when data was written to the partition.<br/>Type: `Timestamp`. |
| `RowReads` | Number of point reads since the start of the partition tablet.<br/>Type: `Uint64`. |
| `RowUpdates` | Number of rows written since the start.<br/>Type: `Uint64`. |
| `RowDeletes` | Number of rows deleted since the start.<br/>Type: `Uint64`. |
| `RangeReads` | Number of row range reads since the start.<br/>Type: `Uint64`. |
| `RangeReadRows` | Number of rows read in the ranges since the start.<br/>Type: `Uint64`. |
| `InFlightTxCount` | Number of in-flight transactions.<br/>Type: `Uint64`. |
| `ImmediateTxCompleted` | Number of one-shard transactions completed since the start.<br/>Type: `Uint64`. |
| `CoordinatedTxCompleted` | Number of coordinated transactions completed since the start.<br/>Type: `Uint64`. |
| `TxRejectedByOverload` | Number of transactions rejected due to overload (since the start).<br/>Type: `Uint64`. |
| `TxRejectedByOutOfStorage` | Number of transactions rejected due to lack of storage space (since the start).<br/>Type: `Uint64`. |

### Example queries

Top 5 of most loaded partitions among all DB tables:

```sql
SELECT
    Path,
    PartIdx,
    CPUCores
FROM `.sys/partition_stats`
ORDER BY CPUCores DESC
LIMIT 5
```

List of DB tables with in-flight sizes and loads:

```sql
SELECT
    Path,
    COUNT(*) as Partitions,
    SUM(RowCount) as Rows,
    SUM(DataSize) as Size,
    SUM(CPUCores) as CPU
FROM `.sys/partition_stats`
GROUP BY Path
```

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
| `IntervalEnd` | The end of a one-minute or one-hour interval.<br/>Type: `Timestamp`.<br/>Key: `0`. |
| `Rank` | Rank of a top query.<br/>Type: `Uint32`.<br/>Key: `1`. |
| `QueryText` | Query text.<br/>Type: `Utf8`. |
| `Duration` | Total query execution time.<br/>Type: `Interval`. |
| `EndTime` | Query execution end time. <br/>Type: `Timestamp`. |
| `Type` | Query type (data, scan, or script).<br/>Type: `String`. |
| `ReadRows` | Number of rows read.<br/>Type: `Uint64`. |
| `ReadBytes` | Number of bytes read.<br/>Type: `Uint64`. |
| `UpdateRows` | Number of rows written.<br/>Type: `Uint64`. |
| `UpdateBytes` | Number of bytes written.<br/>Type: `Uint64`. |
| `DeleteRows` | Number of rows deleted.<br/>Type: `Uint64`. |
| `DeleteBytes` | Number of bytes deleted.<br/>Type: `Uint64`. |
| `Partitions` | Number of table partitions used during query execution.<br/>Type: `Uint64`. |
| `UserSID` | User Security ID.<br/>Type: `String`. |
| `ParametersSize` | Size of query parameters in bytes.<br/>Type: `Uint64`. |
| `CompileDuration` | Duration of query compilation.<br/>Type: `Interval`. |
| `FromQueryCache` | Shows whether the cache of prepared queries was used.<br/>Type: `Bool`. |
| `CPUTime` | Total CPU time used to execute the query (microseconds).<br/>Type: `Uint64`. |
| `ShardCount` | Number of shards used during query execution.<br/>Type: `Uint64`. |
| `SumShardCPUTime` | Total CPU time used in shards.<br/>Type: `Uint64`. |
| `MinShardCPUTime` | Minimum CPU time used in shards.<br/>Type: `Uint64`. |
| `MaxShardCPUTime` | Maximum CPU time used in shards.<br/>Type: `Uint64`. |
| `ComputeNodesCount` | Number of compute nodes used during query execution.<br/>Type: `Uint64`. |
| `SumComputeCPUTime` | Total CPU time used in compute nodes.<br/>Type: `Uint64`. |
| `MinComputeCPUTime` | Minimum CPU time used in compute nodes.<br/>Type: `Uint64`. |
| `MaxComputeCPUTime` | Maximum CPU time used in compute nodes.<br/>Type: `Uint64`. |
| `CompileCPUTime` | CPU time used to compile a query.<br/>Type: `Uint64`. |
| `ProcessCPUTime` | CPU time used for overall query handling.<br/>Type: `Uint64`. |

### Example queries

Top queries by execution time for the last minute when queries were made:

```sql
PRAGMA AnsiInForEmptyOrNullableItemsCollections;
$last = (
    SELECT
        MAX(IntervalEnd)
    FROM `.sys/top_queries_by_duration_one_minute`
);
SELECT
    IntervalEnd,
    Rank,
    QueryText,
    Duration
FROM `.sys/top_queries_by_duration_one_minute`
WHERE IntervalEnd IN $last
```

Queries that read the most bytes, broken down by minute:

```sql
SELECT
    IntervalEnd,
    QueryText,
    ReadBytes,
    ReadRows,
    Partitions
FROM `.sys/top_queries_by_read_bytes_one_minute`
WHERE Rank = 1
```

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
| `IntervalEnd` | The end of a one-minute interval.<br/>Type: `Timestamp`.<br/>Key: `0`. |
| `Rank` | Query rank within an interval (by the SumCPUTime field).<br/>Type: `Uint32`.<br/>Key: `1`. |
| `QueryText` | Query text.<br/>Type: `Utf8`. |
| `Count` | Number of query runs.<br/>Type: `Uint64`. |
| `SumDuration` | Total duration of queries.<br/>Type: `Interval`. |
| `Count` | Number of query runs.<br/>Type: `Uint64`. |
| `SumDuration` | Total duration of queries.<br/>Type: `Interval`. |
| `MinDuration` | Minimum query duration.<br/>Type: `Interval`. |
| `MaxDuration` | Maximum query duration.<br/>Type: `Interval`. |
| `SumCPUTime` | Total CPU time used.<br/>Type: `Uint64`. |
| `MinCPUTime` | Minimum CPU time used.<br/>Type: `Uint64`. |
| `MaxCPUTime` | Maximum CPU time used.<br/>Type: `Uint64`. |
| `SumReadRows` | Total number of rows read.<br/>Type: `Uint64`. |
| `MinReadRows` | Minimum number of rows read.<br/>Type: `Uint64`. |
| `MaxReadRows` | Maximum number of rows read.<br/>Type: `Uint64`. |
| `SumReadBytes` | Total number of bytes read.<br/>Type: `Uint64`. |
| `MinReadBytes` | Minimum number of bytes read.<br/>Type: `Uint64`. |
| `MaxReadBytes` | Maximum number of bytes read.<br/>Type: `Uint64`. |
| `SumUpdateRows` | Total number of rows written.<br/>Type: `Uint64`. |
| `MinUpdateRows` | Minimum number of rows written.<br/>Type: `Uint64`. |
| `MaxUpdateRows` | Maximum number of rows written.<br/>Type: `Uint64`. |
| `SumUpdateBytes` | Total number of bytes written.<br/>Type: `Uint64`. |
| `MinUpdateBytes` | Minimum number of bytes written.<br/>Type: `Uint64`. |
| `MaxUpdateBytes` | Maximum number of bytes written.<br/>Type: `Uint64`. |
| `SumDeleteRows` | Total number of rows deleted.<br/>Type: `Uint64`. |
| `MinDeleteRows` | Minimum number of rows deleted.<br/>Type: `Uint64`. |
| `MaxDeleteRows` | Maximum number of rows deleted.<br/>Type: `Uint64`. |


### Example queries

Top 10 queries for the last 6 hours by the total number of rows updated per minute:

```sql
SELECT
    SumUpdateRows,
    Count,
    QueryText,
    IntervalEnd
FROM `.sys/query_metrics_one_minute`
ORDER BY SumUpdateRows DESC LIMIT 10
```

Recent queries that read the most bytes per minute:

```sql
SELECT
    IntervalEnd,
    SumReadBytes,
    MinReadBytes,
    SumReadBytes / Count as AvgReadBytes,
    MaxReadBytes,
    QueryText
FROM `.sys/query_metrics_one_minute`
WHERE SumReadBytes > 0
ORDER BY IntervalEnd DESC, SumReadBytes DESC
LIMIT 100
```

## History of overloaded partitions {#top-overload-partitions}

The following system views (tables) store the history of points in time when the load on individual DB table partitions was high:

* `top_partitions_one_minute`: The data is split into one-minute intervals, contains the history for the last 6 hours.
* `top_partitions_one_hour`: The data is split into one-hour intervals, contains the history for the last 2 weeks.

These tables contain partitions with peak loads of more than 70% (`CPUCores` > 0.7). Partitions within a single interval are ranked by peak load value.

Both tables have the same set of fields:

| Field | Description |
--- | ---
| `IntervalEnd` | The end of a one-minute or one-hour interval.<br/>Type: `Timestamp`.<br/>Key: `0`. |
| `Rank` | Partition rank within an interval (by CPUCores).<br/>Type: `Uint32`.<br/>Key: `1`. |
| `TabletId` | ID of the tablet serving the partition.<br/>Type: `Uint64`. |
| `Path` | Full path to the table.<br/>Type: `Utf8`. |
| `PeakTime` | Peak time within an interval.<br/>Type: `Timestamp`. |
| `CPUCores` | Peak load per partition (CPU share).<br/>Type: `Double`. |
| `NodeId` | ID of the node where the partition was located during the peak load.<br/>Type: `Uint32`. |
| `DataSize` | Approximate partition size, in bytes, during the peak load.<br/>Type: `Uint64`. |
| `RowCount` | Approximate row count during the peak load.<br/>Type: `Uint64`. |
| `IndexSize` | Partition index size per tablet during the peak load.<br/>Type: `Uint64`. |
| `InFlightTxCount` | The number of in-flight transactions during the peak load.<br/>Type: `Uint32`. |

### Example queries

The following query returns partitions with CPU usage of more than 70% in the specified interval, with tablet IDs and sizes as of the time when the percentage was exceeded. The query is made to the `.sys/top_partitions_one_minute` table with data over the last six hours split into one-minute intervals:

```sql
SELECT
   IntervalEnd,
   CPUCores,
   Path,
   TabletId,
   DataSize
FROM `.sys/top_partitions_one_minute`
WHERE CPUCores > 0.7
AND IntervalEnd BETWEEN Timestamp("YYYY-MM-DDThh:mm:ss.uuuuuuZ") AND Timestamp("YYYY-MM-DDThh:mm:ss.uuuuuuZ")
ORDER BY IntervalEnd desc, CPUCores desc
```

* `"YYYY-MM-DDTHH:MM:SS.UUUUUUZ"`: Time in the UTC 0 zone (`YYYY` stands for year, `MM`, for month, `DD`, for date, `hh`, for hours, `mm`, for minutes, `ss`, for seconds, and `uuuuuu`, for microseconds). For example, `"2023-01-26T13:00:00.000000Z"`.

The following query returns partitions with CPU usage of over 90% in the specified interval, with tablet IDs and sizes as of the time when the percentage was exceeded. The query is made to the `.sys/top_partitions_one_hour` table with data over the last two weeks split into one-hour intervals:

```sql
SELECT
   IntervalEnd,
   CPUCores,
   Path,
   TabletId,
   DataSize
FROM `.sys/top_partitions_one_hour`
WHERE CPUCores > 0.9
AND IntervalEnd BETWEEN Timestamp("YYYY-MM-DDThh:mm:ss.uuuuuuZ") AND Timestamp("YYYY-MM-DDThh:mm:ss.uuuuuuZ")
ORDER BY IntervalEnd desc, CPUCores desc
```

* `"YYYY-MM-DDTHH:MM:SS.UUUUUUZ"`: Time in the UTC 0 zone (`YYYY` stands for year, `MM`, for month, `DD`, for date, `hh`, for hours, `mm`, for minutes, `ss`, for seconds, and `uuuuuu`, for microseconds). For example, `"2023-01-26T13:00:00.000000Z"`.
