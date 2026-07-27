{% if select_command != "SELECT STREAM" %}

## GROUP BY

Groups the results `SELECT` by the values of the specified columns or expressions. Together with `GROUP BY`, [aggregate functions](../../builtins/aggregation.md) (`COUNT`, `MAX`, `MIN`, `SUM`, `AVG`) are often used to perform calculations in each group.

If `GROUP BY` is present in the query, then when selecting columns (between `SELECT ... FROM`), the following constructs are allowed:

1. Columns by which grouping is performed (present in the `GROUP BY` argument).
2. Aggregate functions (see the next section). Columns that are **not** grouped by can only be included as arguments of an aggregate function.
3. Functions that return the start and end time of the current window (`HOP_START` and `HOP_END`) (for `GROUP BY HOP`).
4. Arbitrary computations combining items 1-3.

It is possible to group by the result of computing an arbitrary expression on the source columns. In this case, to access the result of this expression, it is recommended to assign it a name using `AS`, see the second [example](#examples).

### Syntax


```yql
SELECT                             -- In SELECT you can use:
    column1,                       -- key columns specified in GROUP BY
    key_n,                         -- named expressions specified in GROUP BY
    column1 + key_n,               -- arbitrary non-aggregate functions from them
    Aggr_Func1( column2 ),         -- aggregate functions containing any columns in arguments,
    Aggr_Func2( key_n + column2 ), --   including named expressions specified in GROUP BY
    ...
FROM table
GROUP BY
    column1, column2, ...,
    <expr> AS key_n           -- When grouping by an expression, a name can be assigned to it through AS,
                              -- which can be used in SELECT
```


A query of the form `SELECT * FROM table GROUP BY k1, k2, ...` returns all columns listed in GROUP BY, that is, it is equivalent to the query `SELECT DISTINCT k1, k2, ... FROM table`.

The asterisk can also be used as an argument of the aggregate function `COUNT`. `COUNT(*)` means "number of rows in the group".

{% note info %}

Aggregate functions do not take `NULL` into account in their arguments, except for the function `COUNT`.

{% endnote %}

Also, YQL provides a mechanism for aggregate function factories, implemented using the functions [`AGGREGATION_FACTORY`](../../builtins/basic.md#aggregationfactory) and [`AGGREGATE_BY`](../../builtins/aggregation.md#aggregateby).

### Examples {#examples}


```yql
SELECT key, COUNT(*) FROM my_table
GROUP BY key;
```


```yql
SELECT double_key, COUNT(*) FROM my_table
GROUP BY key + key AS double_key;
```


```yql
SELECT
   double_key,                           -- OK: key column
   COUNT(*) AS group_size,               -- OK: COUNT(*)
   SUM(key + subkey) AS sum1,            -- OK: aggregate function
   CAST(SUM(1 + 2) AS String) AS sum2,   -- OK: aggregate function with constant argument
   SUM(SUM(1) + key) AS sum3,            -- ERROR: nested aggregations are not allowed
   key AS k1,                            -- ERROR: use of non-key column key without aggregation
   key * 2 AS dk1,                       -- ERROR in YQL: use of non-key column key without aggregation
FROM my_table
GROUP BY
  key * 2 AS double_key,
  subkey as sk,
```


{% note warning %}

The ability to specify a name for a column or expression in `GROUP BY .. AS foo` is a YQL extension. Such a name becomes visible in `WHERE` even though filtering by `WHERE` is performed [earlier](../index.md#selectexec) than grouping. In particular, if the `T` table has two columns `foo` and `bar`, then in the `SELECT foo FROM T WHERE foo > 0 GROUP BY bar AS foo` query filtering will actually occur on the `bar` column from the original table.

{% endnote %}

## GROUP BY ... SessionWindow() {#session-window}

YQL supports grouping by sessions. A special function `SessionWindow` can be added to regular expressions in `GROUP BY`:


```yql
SELECT
  user,
  session_start,
  SessionStart() AS same_session_start, -- the same as session_start
  COUNT(*) AS session_size,
  SUM(value) AS sum_over_session,
FROM my_table
GROUP BY user, SessionWindow(<time_expr>, <timeout_expr>) AS session_start
```


The following happens:

1. The input table is partitioned by the grouping keys specified in `GROUP BY`, ignoring SessionWindow (in this case, by `user`). If there is nothing else in `GROUP BY` besides SessionWindow, the input table falls into one partition.
2. Each partition is divided into non-overlapping subsets of rows (sessions). For this, the partition is sorted in ascending order of the value of expression `time_expr`. Session boundaries are drawn between adjacent elements of the partition whose difference in values of `time_expr` exceeds `timeout_expr`.
3. The sessions obtained in this way are the final partitions on which aggregate functions are calculated.

The key column of SessionWindow() (in the example, `session_start`) has the value "minimum `time_expr` in the session".
Additionally, when SessionWindow() is present in `GROUP BY`, a special aggregate function [SessionStart](../../builtins/aggregation.md#session-start) can be used.

An extended version of SessionWindow with four arguments is also supported:

`SessionWindow(<order_expr>, <init_lambda>, <update_lambda>, <calculate_lambda>)`

Here:

* `<order_expr>` — expression by which the original partition is sorted
* `<init_lambda>` — lambda function for initializing the session calculation state. It has the signature `(TableRow())->State`. It is called once on the first element (in sort order) of the original partition.
* `<update_lambda>` — lambda function for updating the session calculation state and determining session boundaries. It has the signature `(TableRow(), State)->Tuple<Bool, State>`. It is called on each element of the original partition except the first. The new state value is calculated based on the current table row and the previous state. If the first element of the returned tuple has the value `True`, then a new session starts from the _current_ row. The key of the new session is obtained by applying `<calculate_lambda>` to the second element of the tuple.
* `<calculate_lambda>` is a lambda function for computing the session key (the "value" of SessionWindow(), which is also accessible via SessionStart()). The function has the signature `(TableRow(), State)->SessionKey`. It is called on the first element of a partition (after `<init_lambda>`) and on those elements for which `<update_lambda>` returned `True` as the first element of the tuple. Note that to start a new session, `<calculate_lambda>` must return a value that differs from the previous session key. Sessions with the same keys are not merged. For example, if `<calculate_lambda>` sequentially returns `0, 1, 0, 1`, these will be four different sessions.

Using the extended version of SessionWindow, you can solve, for example, the following problem: split a partition into sessions as in the SessionWindow variant with two arguments, but with the maximum session length limited by a certain constant:

### Example


```yql
$max_len = 1000; -- maximum session length
$timeout = 100; -- timeout (timeout_expr in simplified SessionWindow)

$init = ($row) -> (AsTuple($row.ts, $row.ts)); -- session state - a tuple of 1) the value of the timestamp column ts on the first line of the session and 2) on the current line
$update = ($row, $state) -> {
  $is_end_session = $row.ts - $state.0 > $max_len OR $row.ts - $state.1 > $timeout;
  $new_state = AsTuple(IF($is_end_session, $row.ts, $state.0), $row.ts);
  return AsTuple($is_end_session, $new_state);
};
$calculate = ($row, $state) -> ($row.ts);
SELECT
  user,
  session_start,
  SessionStart() AS same_session_start, -- the same as session_start
  COUNT(*) AS session_size,
  SUM(value) AS sum_over_session,
FROM my_table
GROUP BY user, SessionWindow(ts, $init, $update, $calculate) AS session_start
```


`SessionWindow` can be used in `GROUP BY` only once.

{% if feature_group_by_rollup_cube %}

## ROLLUP, CUBE, and GROUPING SETS {#rollup}

Results of aggregate function calculation as subtotals for groups and grand totals for individual columns or the entire table.

### Syntax


```yql
SELECT
    c1, c2,                          -- columns by which grouping is performed

AGGREGATE_FUNCTION(c3) AS outcome_c  -- aggregate function (SUM, AVG, MIN, MAX, COUNT)

FROM table_name

GROUP BY
    GROUP_BY_EXTENSION(c1, c2)       -- GROUP BY extension: ROLLUP, CUBE or GROUPING SETS
```


* `ROLLUP` groups column values in the order they are listed in the arguments (strictly left to right), generating subtotals for each group and a grand total.
* `CUBE` — groups values for all possible combinations of columns, generates subtotals for each group and a grand total.
* `GROUPING SETS` — sets groups for subtotals.

`ROLLUP`, `CUBE`, and `GROUPING SETS` can be combined with commas.

### GROUPING {#grouping}

In the subtotal, the values of columns that are not involved in calculations are replaced with `NULL`. In the grand total, the values of all columns are replaced with `NULL`. `GROUPING` is a function that allows you to distinguish the original `NULL` values from `NULL` values that were added when generating grand totals and subtotals.

`GROUPING` returns a bitmask:

* `0` — `NULL` for the original empty value.
* `1` — `NULL`, added for intermediate or grand total.

### Example


```yql
SELECT
    column1,
    column2,
    column3,

    CASE GROUPING(
        column1,
        column2,
        column3,
    )
        WHEN 1  THEN "Subtotal: column1 and column2"
        WHEN 3  THEN "Subtotal: column1"
        WHEN 4  THEN "Subtotal: column2 and column3"
        WHEN 6  THEN "Subtotal: column3"
        WHEN 7  THEN "Grand total"
        ELSE         "Individual group"
    END AS subtotal,

    COUNT(*) AS rows_count

FROM my_table

GROUP BY
    ROLLUP(
        column1,
        column2,
        column3
    ),
    GROUPING SETS(
        (column2, column3),
        (column3)
        -- if you add more here (column2), then in total
        -- these ROLLUP and GROUPING SETS would give a result,
        -- similar to CUBE
    )
;
```

{% endif %}

## DISTINCT {#distinct}

Applying [aggregate functions](../../builtins/aggregation.md) only to unique column values.

{% note info %}

Applying `DISTINCT` to computed values is not currently implemented. For this purpose, you can use a [subquery](from.md) or the `GROUP BY ... AS ...` expression.

{% endnote %}

### Example


```yql
SELECT
  key,
  COUNT(DISTINCT value) AS count -- top 3 keys by the number of unique values
FROM my_table
GROUP BY key
ORDER BY count DESC
LIMIT 3;
```


Also, the `DISTINCT` keyword can be used to select unique rows through [`SELECT DISTINCT`](distinct.md).

## COMPACT

The presence of the [SQL hint](../lexer.md#sql-hints) `COMPACT` immediately after the keyword `GROUP` allows more efficient aggregation in cases where the query author knows in advance that none of the aggregation keys will have a large amount of data (on the order of a gigabyte or a million rows). If this assumption does not hold, the operation may fail with an Out of Memory error or start working significantly slower compared to a regular GROUP BY.

Unlike a regular GROUP BY, the Map-side combiner stage and additional Reduce for each field with [DISTINCT](#distinct) aggregation are disabled.

### Example


```yql
SELECT
  key,
  COUNT(DISTINCT value) AS count -- top 3 keys by the number of unique values
FROM my_table
GROUP /*+ COMPACT() */ BY key
ORDER BY count DESC
LIMIT 3;
```

{% endif %}

## GROUP BY ... HOP {#group-by-hop}

`HOP` groups data by overlapping time windows ( [hopping windows](https://en.wikipedia.org/wiki/Window_function_(SQL)#Hopping_window)). It is supported both in [analytical queries on tables](#hop-table) and in [streaming queries on topics](#hop-topic).


```yql
HOP(time_extractor, hop, interval, delay)
```


Where:

- `time_extractor` — SQL expression of type `Timestamp` that defines the event time. A timestamp is computed from each input row, which determines the window assignment.
- `hop` — interval between the starts of adjacent windows in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Durations) format, for example `"PT10S"` (10 seconds).
- `interval` — size (duration) of each window in ISO 8601 format, for example `"PT30S"` (30 seconds).
- — `delay` of window closing after its completion in ISO 8601 format. Used only in streaming queries (ignored when working with tables). For streaming queries, it is recommended to use [HoppingWindow](#group-by-hopping_window) with [watermarks](../../../../dev/streaming-query/watermarks.md) instead of `delay`.

Also available are aggregate functions `HOP_START()` and `HOP_END()`, which return the start and end timestamps of the current window of type `Timestamp`, respectively.

### Description {#hop-description}

Let's break down the algorithm using an example.


```yql
GROUP BY HOP(CAST(ts AS Timestamp), "PT10S", "PT30S", "PT20S")
```


In this example, `CAST(ts AS Timestamp)` extracts the event time from column `ts`. Parameter `hop` equals 10 seconds, `interval` equals 30 seconds, `delay` equals 20 seconds.

Windows are built according to the following rule:

- Window starts are aligned to moments that are multiples of `hop` (10 seconds), starting from 0: 0, 10, 20, and so on.
- The duration of each window is `interval` (30 seconds). The resulting windows are: `[0; 30)`, `[10; 40)`, `[20; 50)`, and so on.
- The event falls into all windows whose time range includes its time. For example, an event with time 25 seconds falls into windows `[0; 30)`, `[10; 40)`, and `[20; 50)`.
- A window is considered complete when an event with a timestamp not less than the end of this window + `delay` (20 seconds) is received. For example, window `[10; 40)` closes when an event with a timestamp of 60 or more is received.

### Analytical `HOP` over a table {#hop-table}

When working with tables, data is grouped by `GROUP BY` keys (ignoring `HOP`), forming groups of rows (hereinafter groups). Inside each group:

1. Rows are sorted in ascending order of `time_extractor`.
2. Each row is assigned to one or more overlapping windows.
3. The specified aggregate functions are computed on each window.

The `delay` parameter is **not used** in analytical table processing: the data is already fully available, the traversal order is determined by sorting by `time_extractor`, and window completion is determined by the full group scan algorithm (see the closing rule in the [description](#hop-description) above).

### Streaming `HOP` over a topic {#hop-topic}

When working with [topics](../../../../concepts/datamodel/topic.md), data is grouped by `GROUP BY` keys (ignoring `HOP`), forming groups. Inside each group:

1. Events are processed in an order close to ascending `time_extractor`. Small deviations from strict ordering are allowed.
2. Each event is assigned to one or more overlapping windows.
3. The specified aggregate functions are computed on each window.

In streaming queries, events may arrive not in strict chronological order. The `delay` parameter sets the waiting time after the formal window completion: the window does not close immediately, but after `delay` seconds, to allow delayed events time to arrive. Events that arrive after the window closes are ignored.

### Limitations

`time_extractor` is an SQL expression that depends only on the input column values and must have type `Timestamp`.

For specifying `hop`, `interval`, and `delay`, a string expression conforming to the [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) standard is used, for example, `PT10S` — 10 seconds, `PT1M` — 1 minute. This is the format used to construct the built-in type `Interval` from a [string](../../builtins/basic.md#data-type-literals).

The values of parameters `interval` and `delay` must be divisible by the value of parameter `hop`. This requirement ensures alignment of window boundaries: each window starts at a time that is a multiple of `hop` and ends exactly after `interval`, guaranteeing uniform coverage of the time axis without gaps. Parameters `hop` and `interval` must be positive.

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

groups events by overlapping time windows (hopping windows), similar to [GROUP BY `HoppingWindow`](#group-by-hop). It is supported both in [analytical queries on tables](#hopping-window-table) and in [streaming queries on topics](#hopping-window-topic). The main difference from `HOP` is that in streaming queries, `HoppingWindow` uses the [watermarks](../../../../dev/streaming-query/watermarks.md) mechanism to determine the window closing time instead of a fixed parameter `delay`.


```yql
HoppingWindow(time_extractor, hop, interval)
```


Where:

- `time_extractor` — SQL expression of type `Timestamp` that defines the event time. It must depend only on input columns.
- `hop` — the step (shift period) between the starts of adjacent windows in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Durations) format, for example `"PT10S"` (10 seconds).
- `interval` — the size (duration) of each window in ISO 8601 format, for example `"PT1M"` (1 minute). The value of `interval` must be divisible by `hop`, because windows are aligned to multiples of the step interval.

The functions `HOP_START()` and `HOP_END()` are also available, returning the timestamps of the start and end of the current window.

The window construction algorithm is the same as [GROUP BY HOP](#group-by-hop): windows start at moments that are multiples of `hop` and have a duration of `interval`. An event falls into all windows whose time range includes its time.

### Analytical `HoppingWindow` over a table {#hopping-window-table}

When working with tables, `HoppingWindow` performs grouping by time windows similarly to [HOP](#group-by-hop), but without the `delay` parameter, which was always ignored in analytical use (the data in the table is already sorted).

1. The input table is partitioned by the grouping keys specified in `GROUP BY`, ignoring `HoppingWindow`.
2. Each partition is sorted by ascending `time_extractor`.
3. Each partition is divided into overlapping subsets of events (windows).
4. The specified aggregate functions are computed on each subset.

### Streaming `HoppingWindow` over a topic {#hopping-window-topic}

When working with [topics](../../../../concepts/datamodel/topic.md), `HoppingWindow` uses [watermarks](../../../../dev/streaming-query/watermarks.md) to determine when a window closes. A window closes when the watermark value is not less than the end of the window. This provides more accurate aggregation results compared to `HOP`, where the window closes at a fixed `delay`.

1. The input topic is partitioned by the grouping keys specified in `GROUP BY`, ignoring `HoppingWindow`.
2. In each partition, the window advances independently of others.
3. Events are processed in an order close to ascending `time_extractor`. Minor reorderings of the input stream order are allowed.
4. Each partition is divided into overlapping subsets of events (windows).
5. A window closes when a watermark is received whose value is not less than the end of the window. After closing, the aggregation result is output.
6. Events that arrive after the window closes are not included in the results.

For `HoppingWindow` to work correctly in streaming mode, you need to configure the watermark in the [WITH](with.md) section of the source. For details: [{#T}](../../../../dev/streaming-query/watermarks.md#configuration).

### Example

Below is streaming reading from a topic: in `SELECT` it is convenient to output the end of the window via `HOP_END()`. For **tables** in an analytical query, `HOP_START()` or `HOP_END()` is more often used, depending on which window boundary you want to show in the result; the meaning of the windows is the same, only the selected label differs.


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


## HAVING {#having}

Filtering the `SELECT` result set based on the results of computing [aggregate functions](../../builtins/aggregation.md). The syntax is similar to the [`WHERE`](where.md) construct.

### Example


```yql
SELECT
    key
FROM my_table
GROUP BY key
HAVING COUNT(value) > 100;
```
