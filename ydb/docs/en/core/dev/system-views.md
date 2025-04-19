# Database system views

You can make queries to special service tables (system views) to monitor the DB status. These tables are accessible from the root of the database tree and use the `.sys` system path prefix.

You can find the corresponding table's primary key field index in the descriptions of available fields below.

DB system views contain:

* [Details of individual DB table partitions](#partitions).
* [Top queries by certain characteristics](#top-queries).
* [Query details](#query-metrics).
* [History of overloaded partitions](#top-overload-partitions).
* [Information about users, groups, and access rights](#auth).

{% note info %}

Frequent access to system views leads to additional load on the database, especially in the case of a large database size. Exceeding the frequency of 1 request per second is not recommended.

{% endnote %}

## Partitions {#partitions}

The following system view stores detailed information about [partitions](../concepts/datamodel/table.md#partitioning) of DB tables:

* `partition_stats`: Contains information about instant metrics and cumulative operation counters. Instant metrics are, for example, CPU load or count of in-flight [transactions](../concepts/transactions.md). Cumulative counters, for example, count the total number of rows read.

The system view is designed to detect various irregularities in the load on a table partition or show the size of table partition data.

Instant metrics (`NodeID`, `AccessTime`, `CPUCores`, etc.) contain instantaneous values.
Cumulative metrics (`RowReads`, `RowUpdate`, `LockAcquired`, etc.) store accumulated values since the last launch (`StartTime`) of the tablet serving the partition.

Table structure:

Column | Description | Data type | Instant/Cumulative
--- | --- | --- | ---
`OwnerId` | ID of the SchemeShard table.<br/>Key: `0`. | `Uint64` | Instant
`PathId` | ID of the SchemeShard path.<br/>Key: `1`. | `Uint64` | Instant
`PartIdx` | Partition sequence number.<br/>Key: `2`. | `Uint64` | Instant
`FollowerId` | ID of the partition tablet [follower](../concepts/glossary.md#tablet-follower). A value of 0 means the leader.<br/>Key: `3`.| `Uint32` | Instant
`DataSize` | Approximate partition size in bytes. | `Uint64` | Instant
`RowCount` | Approximate number of rows. | `Uint64` | Instant
`IndexSize` | Partition index size in bytes. | `Uint64` | Instant
`CPUCores` | Instantaneous value of the load on the partition (the share of the CPU core time spent by the actor of the partition). | `Double` | Instant
`TabletId` | ID of the partition tablet. | `Uint64` | Instant
`Path` | Full path to the table. | `Utf8` | Instant
`NodeId` | ID of the partition node. | `Uint32` | Instant
`StartTime` | Last time of the launch of the partition tablet. | `Timestamp` | Instant
`AccessTime` | Last time of reading from the partition. | `Timestamp` | Instant
`UpdateTime` | Last time of writing to the partition. | `Timestamp` | Instant
`RowReads` | Number of point reads. | `Uint64` | Cumulative
`RowUpdates` | Number of rows written. | `Uint64` | Cumulative
`RowDeletes` | Number of rows deleted. | `Uint64` | Cumulative
`RangeReads` | Number of range reads. | `Uint64` | Cumulative
`RangeReadRows` | Number of rows read in ranges. | `Uint64` | Cumulative
`InFlightTxCount` | Number of in-flight transactions. | `Uint64` | Instant
`ImmediateTxCompleted` | Number of completed [single-shard transactions](../concepts/glossary.md#transactions). | `Uint32` | Cumulative
`CoordinatedTxCompleted` | Number of completed [distributed transactions](../concepts/glossary.md#transactions). | `Uint64` | Cumulative
`TxRejectedByOverload` | Number of transactions cancelled due to [overload](../troubleshooting/performance/queries/overloaded-errors.md). | `Uint64` | Cumulative
`TxRejectedByOutOfStorage` | Number of transactions cancelled due to lack of storage space. | `Uint64` | Cumulative
`LastTtlRunTime` | Launch time of the last TTL erasure procedure | `Timestamp` | Instant
`LastTtlRowsProcessed` | Number of rows checked during the last TTL erasure procedure | `Uint64` | Instant
`LastTtlRowsErased` | Number of rows deleted during the last TTL erasure procedure | `Uint64` | Instant
`LocksAcquired` | Number of [locks](../contributor/datashard-locks-and-change-visibility.md) acquired. | `Uint64` | Cumulative
`LocksWholeShard` | The number of ["whole shard" locks](../contributor/datashard-locks-and-change-visibility.md#limitations) taken. | `Uint64` | Cumulative
`LocksBroken` | Number of [broken locks](../contributor/datashard-locks-and-change-visibility.md#high-level-overview). | `Uint64` | Cumulative

### Example queries {#partitions-examples}

Top 5 of most loaded partitions among all DB tables:

```yql
SELECT
    Path,
    PartIdx,
    CPUCores
FROM `.sys/partition_stats`
ORDER BY CPUCores DESC
LIMIT 5
```

List of DB tables with in-flight sizes and loads:

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

List of DB tables with the largest number of broken locks:

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

The following system views store data for analyzing the user queries.

Maximum total execution time:

* `top_queries_by_duration_one_minute`: data is split into one-minute intervals, contains the history for the last 6 hours;
* `top_queries_by_duration_one_hour`: data is split into one-hour intervals, contains the history for the last 2 weeks.

Maximum number of bytes read from the table:

* `top_queries_by_read_bytes_one_minute`: data is split into one-minute intervals, contains the history for the last 6 hours;
* `top_queries_by_read_bytes_one_hour`: Data is split into one-hour intervals, contains the history for the last 2 weeks.

Maximum CPU time:

* `top_queries_by_cpu_time_one_minute`: Data is split into one-minute intervals, contains the history for the last 6 hours;
* `top_queries_by_cpu_time_one_hour`: Data is split into one-hour intervals, contains the history for the last 2 weeks.

Different runs of a query with the same text are deduplicated. The query with the maximum value of the corresponding metric is included in the output.
Each time interval (minute or hour) contains the TOP 5 queries completed in that time interval.

Fields that provide information about the used CPU time (...`CPUTime`) are expressed in microseconds.

Query text limit is 4 KB.

All tables have the same structure:

| Column | Description |
|--------|-------------|
| `IntervalEnd` | The end of the minute or hour interval for which statistics are collected.<br/>Type: `Timestamp`.<br/>Key: `0`. |
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

### Example queries {#top-queries-examples}

Top queries by execution time. The query is made to the `.sys/top_queries_by_duration_one_minute` view:

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

Queries that read the most bytes. The query is made to the `.sys/top_queries_by_read_bytes_one_minute` view:

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

## Query details {#query-metrics}

The following system view stores detailed information about queries:

* `query_metrics_one_minute`: Data is split into one-minute intervals, contains up to 256 queries for the last 6 hours.

Each table row contains information about a set of queries with identical text that were made during one minute. The table fields provide the minimum, maximum, and total values for each query metric tracked. Within the interval, queries are sorted in descending order of the total CPU time used.

Restrictions:

* Query text limit is 4 KB.
* Statistics may be incomplete if the database is under heavy load.

Table structure:

| Column | Description |
|--------|-------------|
| `IntervalEnd` | The end of the minute interval for which statistics are collected.<br/>Type: `Timestamp`.<br/>Key: `0`. |
| `Rank` | Query rank within an interval (by the `SumCPUTime` field).<br/>Type: `Uint32`.<br/>Key: `1`. |
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

### Example queries {#query-metrics-examples}

Top 10 queries for the last 6 hours by the total number of rows updated per minute:

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

## History of overloaded partitions {#top-overload-partitions}

The following system views (tables) store the history of points in time when the load on individual DB table partitions was high:

* `top_partitions_one_minute`: The data is split into one-minute intervals, contains the history for the last 6 hours.
* `top_partitions_one_hour`: The data is split into one-hour intervals, contains the history for the last 2 weeks.

These views contain partitions with peak loads of more than 70% (`CPUCores` > 0.7). Partitions within a single interval are ranked by peak load value.

The keys of the views are:

* `IntervalEnd` - the moment when the interval is closed;
* `Rank` - the rank of the partition according to the peak load of `CPUCores` in this interval.

For example, if a table has 10 partitions than `top_partitions_one_hour` for the hour interval `"20.12.2024 10:00-11:00"` will return 10 rows sorted in descending order of `CPUCores`. They will have a `Rank` from 1 to 10 and the same `IntervalEnd` `"20.12.2024 11:00"`.

All tables have the same structure:

| Column | Description |
|--------|-------------|
| `IntervalEnd` | The end of the minute or hour interval for which statistics are collected.<br/>Type: `Timestamp`.<br/>Key: `0`. |
| `Rank` | Partition rank within an interval (by `CPUCores`).<br/>Type: `Uint32`.<br/>Key: `1`. |
| `TabletId` | ID of the tablet serving the partition.<br/>Type: `Uint64`. |
| `FollowerId` | ID of the partition tablet [follower](../concepts/glossary.md#tablet-follower).A value of 0 means the leader.<br/>Type: `Uint32` |
| `Path` | Full path to the table.<br/>Type: `Utf8`. |
| `PeakTime` | Peak time within an interval.<br/>Type: `Timestamp`. |
| `CPUCores` | Peak load per partition (share of the CPU core time spent by the actor of the partition).<br/>Type: `Double`. |
| `NodeId` | ID of the node where the partition was located during the peak load.<br/>Type: `Uint32`. |
| `DataSize` | Approximate partition size, in bytes, during the peak load.<br/>Type: `Uint64`. |
| `RowCount` | Approximate row count during the peak load.<br/>Type: `Uint64`. |
| `IndexSize` | Partition index size per tablet during the peak load.<br/>Type: `Uint64`. |
| `InFlightTxCount` | The number of in-flight transactions during the peak load.<br/>Type: `Uint32`. |

### Example queries {#top-overload-partitions-examples}

The following query returns partitions with CPU usage of more than 70% in the specified interval, with tablet IDs and sizes as of the time when the percentage was exceeded. The query is made to the `.sys/top_partitions_one_minute` view:

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

The following query returns partitions with CPU usage of over 90% in the specified interval, with tablet IDs and sizes as of the time when the percentage was exceeded. The query is made to the `.sys/top_partitions_one_hour` view:

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

* `"YYYY-MM-DDTHH:MM:SS.UUUUUUZ"`: Time in the UTC 0 zone (`YYYY` stands for year, `MM`, for month, `DD`, for date, `hh`, for hours, `mm`, for minutes, `ss`, for seconds, and `uuuuuu`, for microseconds). For example, `"2023-01-26T13:00:00.000000Z"`.

## Users, groups, and access rights {#auth}

The following system views contain information about users, access groups, user membership in groups, as well as information about access rights granted to groups or directly to users.
## History of partitions with broken locks {#top-tli-partitions}

The following system views contain a history of moments with a non-zero number of broken [locks](../contributor/datashard-locks-and-change-visibility.md) `LocksBroken` in individual partitions of DB tables:

* `top_partitions_by_tli_one_minute`: The data is split into one-minute intervals, contains the history for the last 6 hours.
* `top_partitions_by_tli_one_hour`: The data is split into one-hour intervals, contains the history for the last 2 weeks.

The views provide the top 10 partitions with a non-zero number of broken locks `LocksBroken`. Within a single interval, partitions are ranked by the number of broken locks `LocksBroken`.

The keys of the views are:

* `IntervalEnd` - the moment of interval closure;
* `Rank` - the rank of the partition by the number of broken locks `LocksBroken` in this interval.

For example, `top_partitions_by_tli_one_hour` for the hourly interval `"20.12.2024 10:00-11:00"` will output 10 rows, sorted in descending order by `LocksBroken`. They will have `Rank` from 1 to 10 and the same `IntervalEnd` `"20.12.2024 11:00"`.

All tables have the same structure:

| Column | Description |
|--------|-------------|
| `IntervalEnd` | The end of the minute or hour interval for which statistics are collected.<br/>Type: `Timestamp`.<br/>Key: `0`. |
| `Rank` | Partition rank within an interval (by `CPUCores`).<br/>Type: `Uint32`.<br/>Key: `1`. |
| `TabletId` | ID of the tablet serving the partition.<br/>Type: `Uint64`. |
| `FollowerId` | ID of the partition tablet [follower](../concepts/glossary.md#tablet-follower).A value of 0 means the leader.<br/>Type: `Uint32` |
| `Path` | Full path to the table.<br/>Type: `Utf8`. |
| `LocksAcquired` | Number of locks acquired "on a range of keys" in this interval.<br />Type: `Uint64`. |
| `LocksWholeShard` | Number of locks acquired "on the entire partition" in this interval.<br />Type: `Uint64`. |
| `LocksBroken` | Number of broken locks in this interval.<br />Type: `Uint64`. |
| `NodeId` | ID of the node where the partition was located during the peak load.<br/>Type: `Uint32`. |
| `DataSize` | Approximate partition size, in bytes, during the peak load.<br/>Type: `Uint64`. |
| `RowCount` | Approximate row count during the peak load.<br/>Type: `Uint64`. |
| `IndexSize` | Partition index size per tablet during the peak load.<br/>Type: `Uint64`. |

### Example queries {#top-tli-partitions-examples}

The following query returns partitions in the specified time interval, with tablet identifiers and the number of broken locks. The query is made to the `.sys/top_partitions_by_tli_one_minute` view:

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

## Auth users, groups, permissions {#auth}

### Auth users {#users}

The `auth_users` view lists internal {{ ydb-short-name }} [users](../concepts/glossary.md#access-user). It does not include users authenticated through external systems such as LDAP.

This view can be fully accessed by administrators, while regular users can only view their own details.

Table structure:

| Column | Description |
|--------|-------------|
| `Sid` | [SID](../concepts/glossary.md#sid) of the user.<br />Type: `Utf8`.<br />Key: `0`. |
| `IsEnabled` | Indicates if login is allowed; used for explicit administrator block. Independent of `IsLockedOut`.<br />Type: `Bool`. |
| `IsLockedOut` | Indicates that user has been automatically deactivated due to exceeding the threshold for unsuccessful authentication attempts. Independent of `IsEnabled`.<br />Type: `Bool`. |
| `CreatedAt` | Timestamp of user creation.<br />Type: `Timestamp`. |
| `LastSuccessfulAttemptAt` | Timestamp of the last successful authentication attempt.<br />Type: `Timestamp`. |
| `LastFailedAttemptAt` | Timestamp of the last failed authentication attempt.<br />Type: `Timestamp`. |
| `FailedAttemptCount` | Number of failed authentication attempts.<br />Type: `Uint32`. |
| `PasswordHash` | JSON string containing password hash, salt, and hash algorithm.<br />Type: `Utf8`. |

### Auth groups

The `auth_groups` view lists [access groups](../concepts/glossary.md#access-group).

This view can be accessed only by administrators.

Table structure:

| Column | Description |
|--------|-------------|
| `Sid` | [SID](../concepts/glossary.md#sid) of the group.<br />Type: `Utf8`.<br />Key: `0`. |

### Auth group members

The `auth_group_members` view lists membership details within [access groups](../concepts/glossary.md#access-group).

This view can be accessed only by administrators.

Table structure:

| Column | Description |
|--------|-------------|
| `GroupSid` | SID of the group.<br />Type: `Utf8`.<br />Key: `0`. |
| `MemberSid` | SID of the group member. This can be either the SID of a user or the SID of a group.<br />Type: `Utf8`.<br />Key: `1`. |

### Auth permissions

The auth permissions views list assigned [access rights](../concepts/glossary.md#access-right).

There are two views:

* `auth_permissions`: Directly assigned access rights.
* `auth_effective_permissions`: Effective access rights, accounting for [inheritance](../concepts/glossary.md#access-right-inheritance).

In this view, the user sees only those [access objects](../concepts/glossary.md#access-object) for which they have the `ydb.granular.describe_schema` permission.

Table structure:

| Column | Description |
|--------|-------------|
| `Path` | Path to the access object.<br />Type: `Utf8`.<br />Key: `0`. |
| `Sid` | SID of the [access subject](../concepts/glossary.md#access-subject).<br />Type: `Utf8`.<br />Key: `1`. |
| `Permission` | Name of the {{ ydb-short-name }} [access right](../yql/reference/syntax/grant.md#permissions-list).<br />Type: `Utf8`.<br />Key: `2`. |

#### Example queries {#auth-permissions-examples}

Retrieving explicitly granted permissions on the access object - table `my_table`:

```yql
SELECT *
FROM `.sys/auth_permissions`
WHERE Path = "my_table"
```

Retrieving effective permissions on the access object - table `my_table`:

```yql
SELECT *
FROM `.sys/auth_effective_permissions`
WHERE Path = "my_table"
```

Retrieving the permissions granted to the user `user3`:

```yql
SELECT *
FROM `.sys/auth_permissions`
WHERE Sid = "user3"
```

### Auth owners

The `auth_owners` view lists details of [access objects](../concepts/glossary.md#access-object) [ownership](../concepts/glossary.md#access-owner).

In this view, the user sees only those [access objects](../concepts/glossary.md#access-object) for which they have the `ydb.granular.describe_schema` permission.

Table structure:

| Column | Description |
|--------|-------------|
| `Path` | Path to the access object.<br />Type: `Utf8`.<br />Key: `0`. |
| `Sid` | SID of the access object owner.<br />Type: `Utf8`. |
