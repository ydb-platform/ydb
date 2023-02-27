## History of overloaded partitions {#top-overload-partitions}

The following system views (tables) store the history of points in time when the load on individual DB table partitions was high:

* `top_partitions_one_minute`: The data is split into one-minute intervals, contains the history for the last 6 hours.
* `top_partitions_one_hour`: The data is split into one-hour intervals, contains the history for the last 2 weeks.

These tables contain partitions with peak loads of more than 70% (`CPUCores` > 0.7). Partitions within a single interval are ranked by peak load value.

Both tables have the same set of fields:

| Field | Description |
--- | ---
| `IntervalEnd` | The end of a one-minute or one-hour interval.<br>Type: `Timestamp`.<br>Key: `0`. |
| `Rank` | Partition rank within an interval (by CPUCores).<br>Type: `Uint32`.<br>Key: `1`. |
| `TabletId` | ID of the tablet serving the partition.<br>Type: `Uint64`. |
| `Path` | Full path to the table.<br>Type: `Utf8`. |
| `PeakTime` | Peak time within an interval.<br>Type: `Timestamp`. |
| `CPUCores` | Peak load per partition (CPU share).<br>Type: `Double`. |
| `NodeId` | ID of the node where the partition was located during the peak load.<br>Type: `Uint32`. |
| `DataSize` | Approximate partition size, in bytes, during the peak load.<br>Type: `Uint64`. |
| `RowCount` | Approximate row count during the peak load.<br>Type: `Uint64`. |
| `IndexSize` | Partition index size per tablet during the peak load.<br>Type: `Uint64`. |
| `InFlightTxCount` | The number of in-flight transactions during the peak load.<br>Type: `Uint32`. |

Examples:

The following query returns partitions with CPU usage of more than 70% in the specified interval, with tablet IDs and sizes as of the time when the percentage was exceeded. The query is made to the `.sys/top_partitions_one_minute` table with data over the last six hours split into one-minute intervals:

> ```yql
> SELECT
>    IntervalEnd,
>    CPUCores,
>    Path,
>    TabletId,
>    DataSize
> FROM `.sys/top_partitions_one_minute`
> WHERE CPUCores > 0.7
> AND IntervalEnd BETWEEN Timestamp("YYYY-MM-DDThh:mm:ss.uuuuuuZ") AND Timestamp("YYYY-MM-DDThh:mm:ss.uuuuuuZ")
> ORDER BY IntervalEnd desc, CPUCores desc
> ```

* `"YYYY-MM-DDTHH:MM:SS.UUUUUUZ"`: Time in the UTC 0 zone (`YYYY` stands for year, `MM`, for month, `DD`, for date, `hh`, for hours, `mm`, for minutes, `ss`, for seconds, and `uuuuuu`, for microseconds). For example, `"2023-01-26T13:00:00.000000Z"`.

The following query returns partitions with CPU usage of over 90% in the specified interval, with tablet IDs and sizes as of the time when the percentage was exceeded. The query is made to the `.sys/top_partitions_one_hour` table with data over the last two weeks split into one-hour intervals:

> ```yql
> SELECT
>    IntervalEnd,
>    CPUCores,
>    Path,
>    TabletId,
>    DataSize
> FROM `.sys/top_partitions_one_hour`
> WHERE CPUCores > 0.9
> AND IntervalEnd BETWEEN Timestamp("YYYY-MM-DDThh:mm:ss.uuuuuuZ") AND Timestamp("YYYY-MM-DDThh:mm:ss.uuuuuuZ")
> ORDER BY IntervalEnd desc, CPUCores desc
> ```

* `"YYYY-MM-DDTHH:MM:SS.UUUUUUZ"`: Time in the UTC 0 zone (`YYYY` stands for year, `MM`, for month, `DD`, for date, `hh`, for hours, `mm`, for minutes, `ss`, for seconds, and `uuuuuu`, for microseconds). For example, `"2023-01-26T13:00:00.000000Z"`.
