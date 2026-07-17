# Database system views

To obtain service information about the database state, you can use system views. They are available from the root of the database tree and use the system path prefix `.sys`.

{% note info %}

Frequent access to system views causes additional load on the database, especially if the database is large. Exceeding the frequency of 1 query per second is not recommended.

{% endnote %}

## Partitions {#partitions}

The following system view stores detailed information about [partitions](../concepts/datamodel/table.md#partitioning) of database tables:

* `partition_stats` — contains information about instantaneous metrics and cumulative operation counters. The former include, for example, CPU load data or the number of executing [transactions](../concepts/transactions.md). The latter include the total number of rows read.

It is intended, for example, for identifying unevenly loaded partitions or displaying the data size in them.

Instantaneous metrics (`NodeId`, `AccessTime`, `CPUCores`, etc.) contain instantaneous values.
Cumulative (non-instantaneous) metrics (`RowReads`, `RowUpdates`, `LocksAcquired`, etc.) store accumulated values since the last start of the tablet (`StartTime`) serving the partition.

View structure:

| Column | Description | Data type | Instantaneous/cumulative |
| --- | --- | --- | --- |
| `OwnerId` | SchemeShard ID of the table.<br/>Key: `0`. | `Uint64` | Instantaneous |
| `PathId` | Path ID in SchemeShard.<br/>Key: `1`. | `Uint64` | Instantaneous |
| `PartIdx` | Partition sequence number.<br/>Key: `2`. | `Uint64` | Instantaneous |
| `FollowerId` | ID of the partition tablet [replica](../concepts/glossary.md#tablet-follower). A value of 0 means the leader.<br/>Key: `3`. | `Uint32` | Instantaneous |
| `DataSize` | Approximate partition data size in bytes. | `Uint64` | Instantaneous |
| `RowCount` | Approximate number of rows. | `Uint64` | Instantaneous |
| `IndexSize` | Partition index size in bytes. | `Uint64` | Instantaneous |
| `CPUCores` | Instantaneous partition load value (fraction of CPU core time spent by the partition actor). | `Double` | Instantaneous |
| `TabletId` | Partition tablet ID. | `Uint64` | Instantaneous |
| `Path` | Full path to the table. | `Utf8` | Instantaneous |
| `NodeId` | ID of the node currently serving the partition. | `Uint32` | Instantaneous |
| `StartTime` | Last start time of the partition tablet. | `Timestamp` | Instantaneous |
| `AccessTime` | Last read time from the partition. | `Timestamp` | Instantaneous |
| `UpdateTime` | Last write time to the partition. | `Timestamp` | Instantaneous |
| `RowReads` | Number of point reads. | `Uint64` | Cumulative |
| `RowUpdates` | Number of rows written. | `Uint64` | Cumulative |
| `RowDeletes` | Number of rows deleted. | `Uint64` | Cumulative |
| `RangeReads` | Number of range reads. | `Uint64` | Cumulative |
| `RangeReadRows` | Number of rows read in ranges. | `Uint64` | Cumulative |
| `InFlightTxCount` | Number of executing transactions. | `Uint64` | Instantaneous |
| `ImmediateTxCompleted` | Number of completed [single-shard transactions](../concepts/glossary.md#transactions). | `Uint32` | Cumulative |
| `CoordinatedTxCompleted` | Number of completed [distributed transactions](../concepts/glossary.md#transactions). | `Uint64` | Cumulative |
| `TxRejectedByOverload` | Number of transactions aborted due to [high load](../troubleshooting/performance/queries/overloaded-errors.md). | `Uint64` | Cumulative |
| `TxRejectedByOutOfStorage` | Number of transactions aborted due to insufficient storage space. | `Uint64` | Cumulative |
| `TxCompleteLag` | Transaction execution latency (how far transactions are behind the scheduled time). | `Interval` | Instantaneous |
| `LastTtlRunTime` | Last start time of TTL partition cleanup | `Timestamp` | Instantaneous |
| `LastTtlRowsProcessed` | Number of partition rows checked during the last TTL cleanup | `Uint64` | Instantaneous |
| `LastTtlRowsErased` | Number of partition rows deleted during the last TTL cleanup | `Uint64` | Instantaneous |
| `LocksAcquired` | Number of [locks](../contributor/datashard-locks-and-change-visibility.md) acquired. | `Uint64` | Cumulative |
| `LocksWholeShard` | Number of [whole-shard locks](../contributor/datashard-locks-and-change-visibility.md#whole-shard-locks) acquired. | `Uint64` | Cumulative |
| `LocksBroken` | Number of [broken locks](../contributor/datashard-locks-and-change-visibility.md#broken-locks). | `Uint64` | Cumulative |

### Query examples {#partitions-examples}

Top 5 most loaded partitions among all database tables:


```yql
SELECT
    Path,
    PartIdx,
    CPUCores
FROM `.sys/partition_stats`
ORDER BY CPUCores DESC
LIMIT 5
```


List of database tables with sizes and current load:


```yql
SELECT
    Path,
    COUNT(*) as Partitions,
    SUM(RowCount) as Rows,
    SUM(DataSize) as Size,
    SUM(CPUCores) as CPU
FROM `.sys/partition_stats`
GROUP BY Path
```


List of database tables with the highest number of broken locks:


```yql
SELECT
    Path,
    COUNT(*) as Partitions,
    SUM(LocksBroken) as TotalLocksBroken
FROM `.sys/partition_stats`
GROUP BY Path
ORDER BY TotalLocksBroken DESC
```


## Top queries {#top-queries}

The following system views store data for analyzing user queries.

Highest total query execution time:

* `top_queries_by_duration_one_minute` — data is split into minute intervals, contains data for the last 6 hours;
* `top_queries_by_duration_one_hour` — data is split into hour intervals, contains data for the last 2 weeks.

Highest number of bytes read from the table:

* `top_queries_by_read_bytes_one_minute` — data is split into minute intervals, contains data for the last 6 hours;
* `top_queries_by_read_bytes_one_hour` — data is split into hour intervals, contains data for the last 2 weeks.

Highest CPU time spent:

* `top_queries_by_cpu_time_one_minute` — data is split into minute intervals, contains data for the last 6 hours;
* `top_queries_by_cpu_time_one_hour` — data is split into hour intervals, contains data for the last 2 weeks.

Queries with the same text are merged, and the query with the maximum value of the corresponding metric is included in the output.
Each time interval (minute or hour) contains the TOP-5 queries executed in that time interval.

Fields providing information about CPU time spent (...`CPUTime`) are expressed in microseconds.

The query text is limited to 10 kilobytes.

All views have the same structure:

| Column | Description |
| --- | --- |
| `IntervalEnd` | The end time of the minute or hour interval for which the statistics were collected.<br/>Type: `Timestamp`.<br/>Key: `0`. |
| `Rank` | Query rank in the top.<br/>Type: `Uint32`.<br/>Key: `1`. |
| `QueryText` | Query text.<br/>Type: `Utf8`. |
| `Duration` | Total query execution time.<br/>Type: `Interval`. |
| `EndTime` | Query end time. <br/>Type: `Timestamp`. |
| `Type` | Query type ("data", "scan", "script").<br/>Type: `String`. |
| `ReadRows` | Number of rows read.<br/>Type: `Uint64`. |
| `ReadBytes` | Number of bytes read.<br/>Type: `Uint64`. |
| `UpdateRows` | Number of rows written.<br/>Type: `Uint64`. |
| `UpdateBytes` | Number of bytes written.<br/>Type: `Uint64`. |
| `DeleteRows` | Number of rows deleted.<br/>Type: `Uint64`. |
| `DeleteBytes` | Number of bytes deleted.<br/>Type: `Uint64`. |
| `Partitions` | Number of table partitions involved in query execution.<br/>Type: `Uint64`. |
| `UserSID` | User security ID.<br/>Type: `String`. |
| `ParametersSize` | Query parameter size in bytes.<br/>Type: `Uint64`. |
| `CompileDuration` | Query compilation duration.<br/>Type: `Interval`. |
| `FromQueryCache` | Whether the prepared query cache was used.<br/>Type: `Bool`. |
| `CPUTime` | Total CPU time used for query execution (microseconds).<br/>Type: `Uint64`. |
| `ShardCount` | Number of shards involved in query execution.<br/>Type: `Uint64`. |
| `SumShardCPUTime` | Total CPU time spent in shards.<br/>Type: `Uint64`. |
| `MinShardCPUTime` | Minimum CPU time spent in shards.<br/>Type: `Uint64`. |
| `MaxShardCPUTime` | Maximum CPU time spent in shards.<br/>Type: `Uint64`. |
| `ComputeNodesCount` | Number of compute nodes involved in query execution.<br/>Type: `Uint64`. |
| `SumComputeCPUTime` | Total CPU time spent in compute nodes.<br/>Type: `Uint64`. |
| `MinComputeCPUTime` | Minimum CPU time spent in compute nodes.<br/>Type: `Uint64`. |
| `MaxComputeCPUTime` | Maximum CPU time spent in compute nodes.<br/>Type: `Uint64`. |
| `CompileCPUTime` | CPU time spent on query compilation.<br/>Type: `Uint64`. |
| `ProcessCPUTime` | CPU time spent on overall query processing.<br/>Type: `Uint64`. |

### Query examples {#top-queries-examples}

Top queries by execution time. The query is executed against the `.sys/top_queries_by_duration_one_minute` view:


```yql
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


Queries that read the most bytes. The query is executed against the `.sys/top_queries_by_read_bytes_one_minute` view:


```yql
SELECT
    IntervalEnd,
    QueryText,
    ReadBytes,
    ReadRows,
    Partitions
FROM `.sys/top_queries_by_read_bytes_one_minute`
WHERE Rank = 1
```


## Detailed query information {#query-metrics}

The following system view contains detailed information about queries:

* `query_metrics_one_minute` — data is split into minute intervals, contains up to 256 queries for the last 6 hours.

Each row of the view contains information about multiple queries with the same text that occurred during the interval. The view fields contain the minimum, maximum, and sum values for each tracked query characteristic. Within an interval, queries are sorted in descending order of total CPU time spent.

Limitations:

* query text is limited to 10 kilobytes;
* statistics may be incomplete if the database is under heavy load.

View structure:

| Column | Description |
| --- | --- |
| `IntervalEnd` | The end time of the minute interval for which the statistics were collected<br/>Type: `Timestamp`.<br/>Key: `0`. |
| `Rank` | Query rank within the interval (by the SumCPUTime field).<br/>Type: `Uint32`.<br/>Key: `1`. |
| `QueryText` | Query text.<br/>Type: `Utf8`. |
| `Count` | Number of query runs.<br/>Type: `Uint64`. |
| `SumDuration` | Total query duration.<br/>Type: `Interval`. |
| `MinDuration` | Minimum query duration.<br/>Type: `Interval`. |
| `MaxDuration` | Maximum query duration.<br/>Type: `Interval`. |
| `SumCPUTime` | Total CPU time spent.<br/>Type: `Uint64`. |
| `MinCPUTime` | Minimum CPU time spent.<br/>Type: `Uint64`. |
| `MaxCPUTime` | Maximum CPU time spent.<br/>Type: `Uint64`. |
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
| `LocksBrokenAsBreaker` | Number of locks broken by this query.<br/>Type: `Uint64`. |
| `LocksBrokenAsVictim` | Number of locks of this query that were broken.<br/>Type: `Uint64`. |

{% note info %}

This statistics does not include:

* Locks broken due to schema changes, TTL, or asynchronous replication
* Locks broken due to partition splitting or merging
* Locks broken during tablet restart
* Locks broken when committing interactive transactions (when COMMIT is executed as a separate query)

{% endnote %}

### Query examples {#query-metrics-examples}

Top 10 queries over the last 6 hours by total number of rows written in a minute interval:


```yql
SELECT
    SumUpdateRows,
    Count,
    QueryText,
    IntervalEnd
FROM `.sys/query_metrics_one_minute`
ORDER BY SumUpdateRows DESC LIMIT 10
```


Recent queries that read the most bytes per minute:


```yql
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


## Detailed information about session queries {#query-sessions}

The following system view contains detailed information about current sessions and the queries running in them.

* `.sys/query_sessions` — contains information about sessions and the queries running in them.

| Column | Description |
| :--- | :--- |
| `SessionId` | Unique session ID.<br/>Type: `Utf8`.<br/>Key: `0`. |
| `NodeId` | ID of the node where the session is running.<br/>Type: `Uint32`. |
| `State` | Current session state.<br/>Type: `Utf8`. |
| `Query` | Text of the last or currently executing query.<br/>Type: `Utf8`. |
| `QueryCount` | Number of queries executed within this session.<br/>Type: `Uint32`. |
| `ClientAddress` | Network address of the client that initiated the session.<br/>Type: `Utf8`. |
| `ClientPID` | Process ID (PID) of the client application.<br/>Type: `Utf8`. |
| `ClientUserAgent` | Client software information (User-Agent).<br/>Type: `Utf8`. |
| `ClientSdkBuildInfo` | Client SDK build information.<br/>Type: `Utf8`. |
| `ApplicationName` | Application name specified by the client on connection.<br/>Type: `Utf8`. |
| `SessionStartAt` | Session start (creation) time.<br/>Type: `Timestamp`. |
| `QueryStartAt` | Start time of the current query execution.<br/>Type: `Timestamp`. |
| `StateChangeAt` | Time of the last session state change.<br/>Type: `Timestamp`. |
| `UserSID` | Security ID of the user who owns the session.<br/>Type: `Utf8`. |
| `WmPoolId` | ID of the Workload Manager pool where the session query is executed.<br/>Type: `Utf8`. |
| `WmState` | Query state in Workload Manager.<br/>Type: `Utf8`. |
| `WmEnterTime` | Time when the query transitioned to PENDING or DELAYED status.<br/>Type: `Timestamp`. |
| `WmExitTime` | Time when the query was submitted for execution.<br/>Type: `Timestamp`. |

Possible values of the `WmState` field:

* `NONE` — Not being processed.
* `PENDING` — Being processed (in classification/routing).
* `DELAYED` — In queue.
* `EXITED` — Submitted for execution.

Possible values of the `State` field:

* `IDLE` — Session is waiting for a query.
* `EXECUTING` — A query is being executed.

### Query examples {#query-sessions-examples}

View all active sessions:


```yql
SELECT * FROM `.sys/query_sessions`
```


Top 20 longest-running queries:


```yql
SELECT
    Query,
    SessionId,
    NodeId,
    QueryStartAt
FROM `.sys/query_sessions`
WHERE State = 'EXECUTING'
ORDER BY QueryStartAt ASC
LIMIT 20
```


Search for application sessions filtered by Workload Manager pool:


```yql
SELECT
    SessionId,
    Query,
    State,
    WmState,
    ClientAddress
FROM `.sys/query_sessions`
WHERE ApplicationName = 'my_analytics_app'
  AND WmPoolId = 'heavy_queries'
```


## Query compilation cache {#compile-cache-queries}

The following system view contains information about queries stored in the compilation cache on all cluster nodes:

* `compile_cache_queries` — contains information about queries in the compilation cache of all cluster nodes.

{% note warning %}

The `compile_cache_queries` system view is not available in serverless mode.

{% endnote %}

Each cluster node has its own cache of compiled queries. This cache is used by all sessions running on that node: if the query text is already in the cache during execution, no additional compilation is required.

The `compile_cache_queries` system view provides data on the cache state on all or selected cluster nodes. When executing a query to this view, requests are sent to each node, unless a restriction on `NodeId` is specified in the `WHERE` condition. The response returns up-to-date information about the cache contents for each node, and the resulting table combines the received data.

View structure:

| Column | Description |
| --- | --- |
| `NodeId` | ID of the node where the query is stored in the cache.<br/>Type: `Uint32`.<br/>Key: `0`. |
| `QueryId` | Unique query ID in the node cache.<br/>Type: `Utf8`.<br/>Key: `1`. |
| `Query` | Query text. If the query exceeds 10 KB, it is truncated.<br/>Type: `Utf8`. |
| `AccessCount` | Number of cache hits for the query text.<br/>Type: `Uint64`. |
| `CompiledAt` | Query compilation time.<br/>Type: `Timestamp`. |
| `UserSID` | Security ID of the user on whose behalf the query was compiled. May be empty for system queries.<br/>Type: `Utf8`. |
| `LastAccessedAt` | Time of the last access to the query compilation result in the cache.<br/>Type: `Timestamp`. |
| `CompilationDurationMs` | Query compilation duration in milliseconds.<br/>Type: `Uint64`. |
| `Warnings` | Warnings that occurred during query compilation.<br/>Type: `Utf8`. |
| `Metadata` | Query parameter types in JSON format. Contains the `parameters` key with parameter names and their types.<br/>Type: `Utf8`. |
| `IsTruncated` | Flag indicating whether the query text was truncated due to exceeding the 10 KB limit.<br/>Type: `Bool`. |
| `QueryType` | Query type, one of:<br/>`QUERY_TYPE_SQL_DML` — Table Service<br/>`QUERY_TYPE_SQL_GENERIC_QUERY` — Query Service<br/>`QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY` — Query Service in concurrent mode<br/>May be empty for old records.<br/>Type: `Utf8`. |
| `Syntax` | Query syntax, one of:<br/>`SYNTAX_YQL_V1` — YQL<br/>`SYNTAX_UNSPECIFIED` — for old records without syntax information<br/>`SYNTAX_PG` — deprecated value for records compiled before the removal of experimental PostgreSQL compatibility; new queries with this syntax are not accepted<br/>Type: `Utf8`. |

### Query examples {#compile-cache-queries-examples}

View all queries in the compilation cache:


```yql
SELECT * FROM `.sys/compile_cache_queries`
```


Top 20 most popular queries by access count from 3 nodes:


```yql
SELECT
    Query,
    SUM(AccessCount) AS Hits
FROM `.sys/compile_cache_queries`
WHERE NodeId IN (50000, 50001, 50003)
GROUP BY Query
ORDER BY Hits DESC
LIMIT 20
```


Activity statistics by user:


```yql
SELECT
    UserSID,
    COUNT(DISTINCT QueryId) AS Plans,
    SUM(AccessCount) AS Hits,
    AVG(CompilationDurationMs) AS AvgCompileMs
FROM `.sys/compile_cache_queries`
GROUP BY UserSID
ORDER BY Hits DESC
```


Search for queries with long compilation:


```yql
SELECT
    Query,
    NodeId,
    CompilationDurationMs,
    AccessCount
FROM `.sys/compile_cache_queries`
WHERE CompilationDurationMs > 1000
ORDER BY CompilationDurationMs DESC
```


## Overloaded partition history {#top-overload-partitions}

The following system views contain the history of high load moments on individual database table partitions:

* `top_partitions_one_minute` — data is split into minute intervals, contains history for the last 6 hours;
* `top_partitions_one_hour` — data is split into hour intervals, contains history for the last 2 weeks.

Partitions with a peak load of more than 70% (`CPUCores` > 0.7) are included in the views. Within one interval, partitions are ranked by the peak load value.

Both views contain the same set of fields:

The view keys are:

* `IntervalEnd` — interval end time;
* `Rank` — partition rank by peak load `CPUCores` in this interval.

For example, if a table has 10 partitions, then `top_partitions_one_hour` for the hour interval `"20.12.2024 10:00-11:00"` will return 10 rows sorted in descending order of `CPUCores`. They will have `Rank` from 1 to 10 and the same `IntervalEnd` `"20.12.2024 11:00"`.

| Column | Description |
| --- | --- |
| `IntervalEnd` | End time of the minute or hour interval for which statistics were collected.<br/>Type: `Timestamp`.<br/>Key: `0`. |
| `Rank` | Partition rank within the interval (by `CPUCores`).<br/>Type: `Uint32`.<br/>Key: `1`. |
| `TabletId` | ID of the tablet serving the partition.<br/>Type: `Uint64`. |
| `FollowerId` | ID of the partition tablet [replica](../concepts/glossary.md#tablet-follower). A value of 0 means the leader.<br/>Type: `Uint32` |
| `Path` | Full path to the table.<br/>Type: `Utf8`. |
| `PeakTime` | Peak value time within the interval.<br/>Type: `Timestamp`. |
| `CPUCores` | Peak partition load value (fraction of CPU core time spent by the partition actor).<br/>Type: `Double`. |
| `NodeId` | ID of the node where the partition was located at the peak time.<br/>Type: `Uint32`. |
| `DataSize` | Approximate partition size in bytes at the peak time.<br/>Type: `Uint64`. |
| `RowCount` | Approximate number of rows at the peak time.<br/>Type: `Uint64`. |
| `IndexSize` | Partition index size in the tablet at the peak time.<br/>Type: `Uint64`. |
| `InFlightTxCount` | Number of transactions in the process of execution at the peak time.<br/>Type: `Uint32`. |

### Query examples {#top-overload-partitions-examples}

The following query outputs partitions with CPU consumption over 70% in the specified time interval, with tablet IDs and their sizes at the time of the spike. The query is executed against the `.sys/top_partitions_one_minute` view, which contains data for the last 6 hours broken down into minute intervals:


```yql
SELECT
    IntervalEnd,
    CPUCores,
    Path,
    TabletId,
    DataSize
FROM `.sys/top_partitions_one_minute`
WHERE CPUCores > 0.7
AND IntervalEnd BETWEEN Timestamp("2000-01-01T00:00:00Z") AND Timestamp("2099-12-31T00:00:00Z")
ORDER BY IntervalEnd desc, CPUCores desc
```


The following query outputs partitions with CPU consumption over 90% in the specified time interval, with tablet IDs and their sizes at the time of the spike. The query is executed against the `.sys/top_partitions_one_hour` view, which contains data for the last 2 weeks broken down into hourly intervals:


```yql
SELECT
    IntervalEnd,
    CPUCores,
    Path,
    TabletId,
    DataSize
FROM `.sys/top_partitions_one_hour`
WHERE CPUCores > 0.9
AND IntervalEnd BETWEEN Timestamp("2000-01-01T00:00:00Z") AND Timestamp("2099-12-31T00:00:00Z")
ORDER BY IntervalEnd desc, CPUCores desc
```


## Partition history with broken locks {#top-tli-partitions}

The following system views contain the history of moments with a non-zero number of broken [locks](../contributor/datashard-locks-and-change-visibility.md) `LocksBroken` in individual partitions of database tables:

* `top_partitions_by_tli_one_minute` — data is broken down into minute intervals, contains history for the last 6 hours;
* `top_partitions_by_tli_one_hour` — data is broken down into hourly intervals, contains history for the last 2 weeks.

The views output the top 10 partitions with a non-zero number of broken locks `LocksBroken`. Within a single interval, partitions are ranked by the number of broken locks `LocksBroken`.

The view keys are:

* `IntervalEnd` — the end time of the interval;
* `Rank` — the rank of the partition by the number of broken locks `LocksBroken` in this interval.

For example, `top_partitions_by_tli_one_hour` for an hourly interval `"20.12.2024 10:00-11:00"` will output 10 rows sorted in descending order of `LocksBroken`. They will have `Rank` from 1 to 10 and the same `IntervalEnd` `"20.12.2024 11:00"`.

Both views contain the same set of fields:

| Column | Description |
| --- | --- |
| `IntervalEnd` | The end time of the minute or hour interval for which the statistics were collected.<br/>Type: `Timestamp`.<br/>Key: `0`. |
| `Rank` | The rank of the partition within the interval (by `LocksBroken`).<br/>Type: `Uint32`.<br/>Key: `1`. |
| `TabletId` | ID of the tablet serving the partition.<br/>Type: `Uint64`. |
| `FollowerId` | ID of the [replica](../concepts/glossary.md#tablet-follower) of the tablet of the partition. A value of 0 means the leader.<br/>Type: `Uint32` |
| `Path` | Full path to the table.<br/>Type: `Utf8`. |
| `LocksAcquired` | Number of "key range" locks set in this interval.<br/>Type: `Uint64`. |
| `LocksWholeShard` | Number of "whole partition" locks set in this interval.<br/>Type: `Uint64`. |
| `LocksBroken` | Number of broken locks in this interval.<br/>Type: `Uint64`. |
| `NodeId` | ID of the node where the partition was located at the time of the peak.<br/>Type: `Uint32`. |
| `DataSize` | Approximate size of the partition in bytes at the time of the peak.<br/>Type: `Uint64`. |
| `RowCount` | Approximate number of rows at the time of the peak.<br/>Type: `Uint64`. |
| `IndexSize` | Size of the partition index in the tablet at the time of the peak.<br/>Type: `Uint64`. |

### Query examples {#top-tli-partitions-examples}

The following query outputs partitions in the specified time interval, with tablet IDs and the number of broken locks. The query is executed against the `.sys/top_partitions_by_tli_one_minute` view:


```yql
SELECT
    IntervalEnd,
    LocksBroken,
    Path,
    TabletId
FROM `.sys/top_partitions_by_tli_one_hour`
WHERE IntervalEnd BETWEEN Timestamp("2000-01-01T00:00:00Z") AND Timestamp("2099-12-31T00:00:00Z")
ORDER BY IntervalEnd desc, LocksBroken desc
```


{% if feature_resource_pool %}

## Information about resource pools {#resource_pools}

The `resource_pools` system view contains information about the [settings](../yql/reference/syntax/create-resource-pool.md#parameters) of [resource pools](../concepts/glossary.md#resource-pool).

Structure of the system view:

| Column | Description |
| --- | --- |
| `Name` | Name of the resource pool.<br/>Type: `Utf8`.<br/>Key: `0`. |
| `ConcurrentQueryLimit` | Maximum number of concurrently executing queries in the resource pool.<br/>Type: `Int32`. |
| `QueueSize` | Maximum size of the wait queue.<br/>Type: `Int32`. |
| `DatabaseLoadCpuThreshold` | CPU load threshold for the entire database, in percent, after which queries are not sent for execution and remain in the queue.<br/>Type: `Double`. |
| `ResourceWeight` | [Weights](../dev/resource-consumption-management.md#resources_weight) for distributing resources among pools.<br/>Type: `Double`. |
| `TotalCpuLimitPercentPerNode` | Percentage of available CPU that all queries on the node can use in this resource pool.<br/>Type: `Double`. |
| `QueryCpuLimitPercentPerNode` | Percentage of available CPU on the node for a single query in the resource pool.<br/>Type: `Double`. |
| `QueryMemoryLimitPercentPerNode` | Percentage of available memory on the node that a query can use in this resource pool.<br/>Type: `Double`. |

### Example {#resource_pools-examples}

The following query outputs information about the settings of a resource pool named `default`:


```yql
SELECT
    Name,
    ConcurrentQueryLimit,
    QueueSize,
    DatabaseLoadCpuThreshold,
    ResourceWeight,
    TotalCpuLimitPercentPerNode,
    QueryCpuLimitPercentPerNode,
    QueryMemoryLimitPercentPerNode
FROM `.sys/resource_pools`
WHERE Name = "default";
```

{% endif %}

{% if feature_resource_pool_classifier %}

## Information about resource pool classifiers {#resource_pools_classifiers}

The `resource_pools_classifiers` system view contains information about the [settings](../yql/reference/syntax/create-resource-pool-classifier.md#parameters) of [resource pool classifiers](../concepts/glossary.md#resource-pool-classifier).

Structure of the system view:

| Column | Description |
| --- | --- |
| `Name` | Name of the resource pool classifier.<br/>Type: `Utf8`.<br/>Key: `0`. |
| `Rank` | Priority for selecting the resource pool classifier.<br/>Type: `Int64`. |
| `MemberName` | User or group of users that will be sent to the specified resource pool.<br/>Type: `Utf8`. |
| `ResourcePool` | The name of the resource pool to which queries will be sent.<br/>Type: `Utf8`. |

### Example {#resource_pools_classifiers-examples}

The following query outputs information about the settings of a resource pool classifier named `olap`:


```yql
SELECT
    Name,
    Rank,
    MemberName,
    ResourcePool
FROM `.sys/resource_pools_classifiers`
WHERE Name = "olap";
```

{% endif %}

## Users, groups, and access rights {#auth}

The following system views contain information about users, access groups, user membership in groups, and access rights granted to groups or directly to users.

### User information {#users}

The `auth_users` view contains a list of local [users](../concepts/glossary.md#access-user) {{ ydb-short-name }}. It does not include users authenticated through external systems such as LDAP.

Administrators have full access to this view. Regular users can only view their own data.

Table structure:

| Column | Description |
| --- | --- |
| `Sid` | User [SID](../concepts/glossary.md#sid).<br />Type: `Utf8`.<br />Key: `0`. |
| `IsEnabled` | Indicates whether login is allowed for this user; used for explicit blocking by an administrator. Independent of `IsLockedOut`.<br />Type: `Bool`. |
| `IsLockedOut` | Indicates that this user is automatically blocked due to exceeding the number of failed authentications. Independent of `IsEnabled`.<br />Type: `Bool`. |
| `CreatedAt` | User creation time.<br />Type: `Timestamp`. |
| `LastSuccessfulAttemptAt` | Time of the last successful authentication.<br />Type: `Timestamp`. |
| `LastFailedAttemptAt` | Time of the last failed authentication.<br />Type: `Timestamp`. |
| `FailedAttemptCount` | Number of failed authentications.<br />Type: `Uint32`. |
| `PasswordHash` | JSON string containing the password hash, salt, and hashing algorithm.<br />Type: `Utf8`. |

### Group information

The `auth_groups` view contains a list of [access groups](../concepts/glossary.md#access-group).

Only administrators have access to this view.

Table structure:

| Column | Description |
| --- | --- |
| `Sid` | Group [SID](../concepts/glossary.md#sid).<br />Type: `Utf8`.<br />Key: `0`. |

### Group membership information

The `auth_group_members` view contains information about membership in [access groups](../concepts/glossary.md#access-group).

Only administrators have access to this view.

Table structure:

| Column | Description |
| --- | --- |
| `GroupSid` | Group SID.<br />Type: `Utf8`.<br />Key: `0`. |
| `MemberSid` | SID of the group member. Can be either a user SID or a group SID.<br />Type: `Utf8`.<br />Key: `1`. |

### Access rights information

The views contain a list of granted [access rights](../concepts/glossary.md#access-right).

Includes two views:

* `auth_permissions`: Explicitly granted access rights.
* `auth_effective_permissions`: Effective access rights considering [inheritance](../concepts/glossary.md#access-right-inheritance).

In this view, a user only sees those [access objects](../concepts/glossary.md#access-object) for which they have the `ydb.granular.describe_schema` right.

Table structure:

| Column | Description |
| --- | --- |
| `Path` | Path to the access object.<br />Type: `Utf8`.<br />Key: `0`. |
| `Sid` | SID of the [access subject](../concepts/glossary.md#access-subject).<br />Type: `Utf8`.<br />Key: `1`. |
| `Permission` | Name of the [access right](../yql/reference/syntax/grant.md#permissions-list) {{ ydb-short-name }}.<br />Type: `Utf8`.<br />Key: `2`. |

#### Query examples

Getting explicitly granted rights on an access object - the `my_table` table:


```yql
SELECT *
FROM `.sys/auth_permissions`
WHERE Path = "my_table"
```


Getting effective rights on an access object - the `my_table` table:


```yql
SELECT *
FROM `.sys/auth_effective_permissions`
WHERE Path = "my_table"
```


Getting rights granted to user `user3`:


```yql
SELECT *
FROM `.sys/auth_permissions`
WHERE Sid = "user3"
```


### Information about access object owners {#auth-owners}

The `auth_owners` view displays information about [owners](../concepts/glossary.md#access-owner) of [access objects](../concepts/glossary.md#access-object).

In this view, a user only sees those [access objects](../concepts/glossary.md#access-object) for which they have been granted the `ydb.granular.describe_schema` right.

Table structure:

| Column | Description |
| --- | --- |
| `Path` | Path to the access object.<br />Type: `Utf8`.<br />Key: `0`. |
| `Sid` | SID of the access object owner.<br />Type: `Utf8`. |

## Streaming queries {#streaming}

### Viewing information about streaming queries {#streaming_queries}

The `streaming_queries` system view contains information about all created [streaming queries](../concepts/streaming-query/streaming-query.md).

In this view, a user only sees those [streaming queries](../concepts/streaming-query/streaming-query.md) for which they have been granted the `ydb.granular.describe_schema` right.

Table structure:

| Column | Description |
| --- | --- |
| `Path` | Full path to the query.<br />Type: `Utf8`.<br />Key: `0`. |
| `Status` | Query execution status, one of:<br />`CREATING` - query is being created <br />`CREATED` - query created but not started <br />`STARTING` - query is starting <br />`RUNNING` - query is running <br />`STOPPING` - query is stopping <br />`STOPPED` - query stopped <br />`SUSPENDED` - query completed with an error and is waiting for a backoff retry <br />Type: `Utf8` |
| `Issues` | Query execution errors in JSON format<br />Type: `Utf8` |
| `Plan` | Query plan in JSON format<br />Type: `Utf8` |
| `Ast` | Query AST<br />Type: `Utf8` |
| `Text` | Query text<br />Type: `Utf8` |
| `Run` | Whether the query is currently being run by the user<br />Type: `Bool` |
| `ResourcePool` | Name of the resource pool the query was bound to ( [see example](../yql/reference/syntax/create-streaming-query.md#examples))<br />Type: `Utf8` |
| `RetryCount` | Number of query restarts<br />Type: `Uint64` |
| `LastFailAt` | Time of the last query execution error<br />Type: `Timestamp` |
| `SuspendedUntil` | Time when an attempt will be made to resume a stopped query<br />Type: `Timestamp` |
