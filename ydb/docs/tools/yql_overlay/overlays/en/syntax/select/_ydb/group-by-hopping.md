## GROUP BY ... HOP {#group-by-hop}

`HOP` groups data by overlapping time windows ( [hopping windows](https://en.wikipedia.org/wiki/Window_function_(SQL)#Hopping_window)). It is supported both in [analytical queries to tables](#hop-table) and in [streaming queries to topics](#hop-topic).


```yql
HOP(time_extractor, hop, interval, delay)
```


Where:

- `time_extractor` — SQL expression of type `Timestamp` that defines the event time. A timestamp is calculated from each input row, which determines the window assignment.
- `hop`: step between the starts of adjacent windows in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Durations) format, for example `"PT10S"` (10 seconds).
- `interval` — the size (duration) of each window in ISO 8601 format, for example `"PT30S"` (30 seconds).
- — `delay` for closing a window after its completion in ISO 8601 format. Used only in streaming queries (ignored when working with tables). For streaming queries, it is recommended to use [HoppingWindow](#group-by-hopping_window) with [watermarks](../../../../../dev/streaming-query/watermarks.md) instead of `delay`.

Also available are aggregate functions `HOP_START()` and `HOP_END()`, which return the start and end timestamps of the current window of type `Timestamp` respectively.

### Description {#hop-description}

Let's break down the algorithm using an example.


```yql
GROUP BY HOP(CAST(ts AS Timestamp), "PT10S", "PT30S", "PT20S")
```


In this example, `CAST(ts AS Timestamp)` extracts the event time from the `ts` column. The `hop` parameter is 10 seconds, `interval` is 30 seconds, `delay` is 20 seconds.

Windows are constructed according to the following rule:

- Window starts are aligned to moments that are multiples of `hop` (10 seconds), starting from 0: 0, 10, 20, and so on.
- The duration of each window is `interval` (30 seconds). The resulting windows are: `[0; 30)`, `[10; 40)`, `[20; 50)`, and so on.
- An event falls into all windows whose time range includes its time. For example, an event with a time of 25 seconds falls into windows `[0; 30)`, `[10; 40)`, and `[20; 50)`.
- A window is considered completed when an event with a timestamp not less than the end of this window + `delay` (20 seconds) is received. For example, window `[10; 40)` closes when an event with a timestamp of 60 or more is received.

### Analytical `HOP` over a table {#hop-table}

When working with tables, data is grouped by `GROUP BY` keys (ignoring `HOP`), forming groups of rows (hereinafter groups). Inside each group:

1. Rows are sorted in ascending order of `time_extractor`.
2. Each row is assigned to one or more overlapping windows.
3. On each window, the specified aggregate functions are computed.

The `delay` parameter is **not used** in analytical table processing: the data is already fully available, the traversal order is set by sorting by `time_extractor`, and the window completion is determined by the full group scan algorithm (see the closing rule in the [description](#hop-description) above).

### Streaming `HOP` over a topic {#hop-topic}

When working with [topics](../../../../../concepts/datamodel/topic.md), data is grouped by `GROUP BY` keys (ignoring `HOP`), forming groups. Inside each group:

1. Events are processed in an order close to ascending `time_extractor`. Small deviations from strict order are allowed.
2. Each event is assigned to one or more overlapping windows.
3. On each window, the specified aggregate functions are computed.

In streaming queries, events may arrive not in strict chronological order. The `delay` parameter sets the waiting time after the formal window completion: the window does not close immediately, but after `delay` seconds, to give delayed events time to arrive. Events that arrive after the window is closed are ignored.

### Limitations

`time_extractor` is an SQL expression that depends only on the input column values and must have type `Timestamp`.

To specify `hop`, `interval`, and `delay`, a string expression conforming to the [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) standard is used, for example, `PT10S` — 10 seconds, `PT1M` — 1 minute. This is the format used to construct the built-in type `Interval` from a [string](../../../builtins/basic.md#data-type-literals).

The values of parameters `interval` and `delay` must be divisible by the value of parameter `hop`. This requirement ensures alignment of window boundaries: each window starts at a time that is a multiple of `hop` and ends exactly after `interval`, which guarantees uniform coverage of the time axis without gaps. Parameters `hop` and `interval` must be positive.

### Examples


```yql
SELECT
    sensor_id,
    HOP_END() AS window_end,
    AVG(temperature) AS avg_temp,
    COUNT(*) AS event_count
FROM sensor_data
GROUP BY
    sensor_id,
    HOP(CAST(event_time AS Timestamp), "PT10S", "PT1M", "PT30S");
```


## GROUP BY ... HoppingWindow {#group-by-hopping_window}

groups events by overlapping time windows (hopping windows), similar to [GROUP BY `HoppingWindow`](#group-by-hop). It is supported both in [analytical queries to tables](#hopping-window-table) and in [stream queries to topics](#hopping-window-topic). The main difference from `HOP` is that in stream queries, `HoppingWindow` uses the [watermarks](../../../../../dev/streaming-query/watermarks.md) mechanism to determine when to close a window instead of a fixed parameter `delay`.


```yql
HoppingWindow(time_extractor, hop, interval)
```


Where:

- `time_extractor`: an SQL expression of type `Timestamp` that defines the event time. It must depend only on input columns.
- `hop`: the step (shift period) between the starts of adjacent windows in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Durations) format, for example `"PT10S"` (10 seconds).
- `interval`: the size (duration) of each window in ISO 8601 format, for example `"PT1M"` (1 minute). The value `interval` must be divisible by `hop`, because windows are aligned to multiples of the step interval.

The functions `HOP_START()` and `HOP_END()` are also available, returning timestamps of the start and end of the current window.

The window construction algorithm is the same as [GROUP BY HOP](#group-by-hop): windows start at moments that are multiples of `hop` and have a duration of `interval`. An event falls into all windows whose time range includes its time.

### Analytical `HoppingWindow` over a table {#hopping-window-table}

When working with tables, `HoppingWindow` performs grouping by time windows similarly to [HOP](#group-by-hop), but without the `delay` parameter, which was always ignored in analytical use (the data in the table is already sorted).

1. The input table is partitioned by the grouping keys specified in `GROUP BY`, ignoring `HoppingWindow`.
2. Each partition is sorted by ascending `time_extractor`.
3. Each partition is divided into overlapping subsets of events (windows).
4. The specified aggregate functions are computed on each subset.

### Streaming `HoppingWindow` over a topic {#hopping-window-topic}

When working with [topics](../../../../../concepts/datamodel/topic.md), `HoppingWindow` uses [watermarks](../../../../../dev/streaming-query/watermarks.md) to determine when to close a window. A window is closed when the watermark value is not less than the end of the window. This provides more accurate aggregation results compared to `HOP`, where the window is closed at a fixed `delay`.

1. The input topic is partitioned by the grouping keys specified in `GROUP BY`, ignoring `HoppingWindow`.
2. In each partition, the window advances independently of others.
3. Events are processed in an order close to ascending `time_extractor`. Minor reorderings of the input stream order are allowed.
4. Each partition is divided into overlapping subsets of events (windows).
5. A window is closed when a watermark is received whose value is not less than the end of the window. After closing, the aggregation result is output.
6. Events that arrive after the window is closed are not included in the results.

For `HoppingWindow` to work correctly in streaming mode, you need to configure the watermark in the [WITH](../with.md) section of the source. For more information: [{#T}](../../../../../dev/streaming-query/watermarks.md#configuration).

### Example

Below is streaming reading from a topic: in `SELECT` it is convenient to output the end of the window via `HOP_END()`. For **tables** in an analytical query, `HOP_START()` or `HOP_END()` are more often used, depending on which window boundary needs to be shown in the result; the meaning of the windows is the same, only the selected label differs.


```yql
SELECT
    key,
    HOP_END() AS window_end,
    COUNT(*) AS event_count
FROM
    my_topic
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        key String,
        event_time String
    ),
    WATERMARK = __ydb_write_time - Interval("PT5S")
)
GROUP BY
    key,
    HoppingWindow(__ydb_write_time, "PT10S", "PT1M");
```
