{% if select_command != "SELECT STREAM" %}

## GROUP BY

Groups the results `SELECT` by the values of the specified columns or expressions. Together with `GROUP BY`, [aggregate functions](../../builtins/aggregation.md) (`COUNT`, `MAX`, `MIN`, `SUM`, `AVG`) are often used to perform calculations in each group.

If `GROUP BY` is present in the query, the following constructs are allowed when selecting columns (between `SELECT ... FROM`):

1. Columns used for grouping (present in the `GROUP BY` argument).
2. Aggregate functions (see the next section). Columns that are **not** used for grouping can only be included as arguments of an aggregate function.
3. Functions that return the start and end time of the current window (`HOP_START` and `HOP_END`) (for `GROUP BY HOP`).
4. Arbitrary computations combining items 1-3.

You can group by the result of computing an arbitrary expression on source columns. In this case, it is recommended to assign a name to the result using `AS`, see the second [example](#examples).

### Syntax


```yql
SELECT                             -- В SELECT можно использовать:
    column1,                       -- ключевые колонки, заданные в GROUP BY
    key_n,                         -- именованные выражения, заданные в GROUP BY
    column1 + key_n,               -- произвольные неагрегатные функции от них
    Aggr_Func1( column2 ),         -- агрегатные функции, содержащие в аргументах любые колонки,
    Aggr_Func2( key_n + column2 ), --   включая именованные выражения, заданные в GROUP BY
    ...
FROM table
GROUP BY
    column1, column2, ...,
    <expr> AS key_n           -- При группировке по выражению ему может быть задано имя через AS,
                              -- которое может быть использовано в SELECT
```


A query like `SELECT * FROM table GROUP BY k1, k2, ...` returns all columns listed in GROUP BY, that is, it is equivalent to the query `SELECT DISTINCT k1, k2, ... FROM table`.

The asterisk can also be used as an argument of the aggregate function `COUNT`. `COUNT(*)` means "number of rows in the group".

{% note info %}

Aggregate functions ignore `NULL` in their arguments, except for the `COUNT` function.

{% endnote %}

YQL also provides an aggregate function factory mechanism implemented using the [`AGGREGATION_FACTORY`](../../builtins/basic.md#aggregationfactory) and [`AGGREGATE_BY`](../../builtins/aggregation.md#aggregateby) functions.

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
   double_key,                           -- ОК: ключевая колонка
   COUNT(*) AS group_size,               -- OK: COUNT(*)
   SUM(key + subkey) AS sum1,            -- ОК: агрегатная функция
   CAST(SUM(1 + 2) AS String) AS sum2,   -- ОК: агрегатная функция с константным аргументом
   SUM(SUM(1) + key) AS sum3,            -- ОШИБКА: вложенные агрегации не допускаются
   key AS k1,                            -- ОШИБКА: использование неключевой колонки key без агрегации
   key * 2 AS dk1,                       -- ОШИБКА в YQL: использование неключевой колонки key без агрегации
FROM my_table
GROUP BY
  key * 2 AS double_key,
  subkey as sk,

```


{% note warning %}

The ability to specify a name for a column or expression in `GROUP BY .. AS foo` is a YQL extension. Such a name becomes visible in `WHERE` despite the fact that filtering by `WHERE` is performed [earlier](../index.md#selectexec) than grouping. In particular, if the `T` table has two columns `foo` and `bar`, then in the `SELECT foo FROM T WHERE foo > 0 GROUP BY bar AS foo` query, filtering will actually be performed on the `bar` column from the original table.

{% endnote %}

## GROUP BY ... SessionWindow() {#session-window}

YQL supports grouping by sessions. In addition to regular expressions in `GROUP BY`, you can add a special function `SessionWindow`:


```yql
SELECT
  user,
  session_start,
  SessionStart() AS same_session_start, -- то же что и session_start
  COUNT(*) AS session_size,
  SUM(value) AS sum_over_session,
FROM my_table
GROUP BY user, SessionWindow(<time_expr>, <timeout_expr>) AS session_start
```


This works as follows:

1. The input table is partitioned by the grouping keys specified in `GROUP BY`, ignoring SessionWindow (in this case, by `user`). If `GROUP BY` contains nothing except SessionWindow, the entire input table falls into a single partition
2. Each partition is divided into non-overlapping subsets of rows (sessions). To do this, the partition is sorted in ascending order of the `time_expr` expression. Session boundaries are drawn between adjacent elements of the partition whose `time_expr` values differ by more than `timeout_expr`
3. The sessions obtained in this way are the final partitions on which aggregate functions are computed.

The key column of SessionWindow() (`session_start` in the example) has the value "minimum `time_expr` in the session".
Additionally, when SessionWindow() is present in `GROUP BY`, you can use the special aggregate function [SessionStart](../../builtins/aggregation.md#session-start).

An extended version of SessionWindow with four arguments is also supported:

`SessionWindow(<order_expr>, <init_lambda>, <update_lambda>, <calculate_lambda>)`

Where:

* `<order_expr>` – expression by which the original partition is sorted
* `<init_lambda>` – lambda function for initializing the session calculation state. Has the signature `(TableRow())->State`. Called once on the first (in sort order) element of the original partition
* `<update_lambda>` – lambda function for updating the session calculation state and determining session boundaries. Has the signature `(TableRow(), State)->Tuple<Bool, State>`. Called on every element of the original partition except the first. The new state value is computed based on the current table row and the previous state. If the first element of the returned tuple has the value `True`, a new session starts from the _current_ row. The key of the new session is obtained by applying `<calculate_lambda>` to the second element of the tuple.
* `<calculate_lambda>` – a lambda function for computing the session key (the "value" of SessionWindow(), also accessible via SessionStart()). The function has the signature `(TableRow(), State)->SessionKey`. It is called on the first element of a partition (after `<init_lambda>`) and on those elements for which `<update_lambda>` returned `True` as the first element of the tuple. It is worth noting that to start a new session, `<calculate_lambda>` must return a value different from the previous session key. In this case, sessions with the same keys are not merged. For example, if `<calculate_lambda>` sequentially returns `0, 1, 0, 1`, these will be four different sessions.

Using the extended version of SessionWindow, you can solve, for example, the following problem: split a partition into sessions as in the SessionWindow variant with two arguments, but with the maximum session length limited by some constant:

### Example


```yql
$max_len = 1000; -- максимальная длина сессии
$timeout = 100; -- таймаут (timeout_expr в упрощенном варианте SessionWindow)

$init = ($row) -> (AsTuple($row.ts, $row.ts)); -- состояние сессии - тапл из 1) значения временной колонки ts на первой строчке сессии и 2) на текущей строчке
$update = ($row, $state) -> {
  $is_end_session = $row.ts - $state.0 > $max_len OR $row.ts - $state.1 > $timeout;
  $new_state = AsTuple(IF($is_end_session, $row.ts, $state.0), $row.ts);
  return AsTuple($is_end_session, $new_state);
};
$calculate = ($row, $state) -> ($row.ts);
SELECT
  user,
  session_start,
  SessionStart() AS same_session_start, -- то же что и session_start
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
    c1, c2,                          -- столбцы, по которым производится группировка

AGGREGATE_FUNCTION(c3) AS outcome_c  -- агрегатная функция (SUM, AVG, MIN, MAX, COUNT)

FROM table_name

GROUP BY
    GROUP_BY_EXTENSION(c1, c2)       -- расширение GROUP BY: ROLLUP, CUBE или GROUPING SETS
```


* `ROLLUP` — groups column values in the order they are listed in the arguments (strictly left to right), generates subtotals for each group, and a grand total.
* `CUBE` — groups values for all possible column combinations, produces subtotals for each group, and a grand total.
* `GROUPING SETS` — defines groups for subtotals.

`ROLLUP`, `CUBE`, and `GROUPING SETS` can be combined with a comma.

### GROUPING {#grouping}

In subtotals, the values of columns not involved in calculations are replaced with `NULL`. In the grand total, the values of all columns are replaced with `NULL`. `GROUPING` is a function that allows you to distinguish the original `NULL` values from `NULL` values that were added when generating subtotals and grand totals.

`GROUPING` returns a bitmask:

* `0` — `NULL` for the original empty value.
* `1` — `NULL` added for an intermediate or grand total.

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
        -- если добавить сюда ещё (column2), то в сумме
        -- эти ROLLUP и GROUPING SETS дали бы результат,
        -- аналогичный CUBE
    )
;
```

{% endif %}

## DISTINCT {#distinct}

Applying [aggregate functions](../../builtins/aggregation.md) only to unique column values.

{% note info %}

Applying `DISTINCT` to computed values is not currently implemented. To do this, you can use a [subquery](from.md) or the `GROUP BY ... AS ...` expression.

{% endnote %}

### Example


```yql
SELECT
  key,
  COUNT(DISTINCT value) AS count -- топ-3 ключей по количеству уникальных значений
FROM my_table
GROUP BY key
ORDER BY count DESC
LIMIT 3;
```


The keyword `DISTINCT` can also be used to select distinct rows via [`SELECT DISTINCT`](distinct.md).

## COMPACT

Using an [SQL hint](../lexer.md#sql-hints) `COMPACT` immediately after the keyword `GROUP` allows for more efficient aggregation when the query author knows in advance that none of the aggregation keys will have a large amount of data (on the order of a gigabyte or a million rows). If this assumption proves incorrect, the operation may fail with an Out of Memory error or run significantly slower compared to a regular GROUP BY.

Unlike a regular GROUP BY, the Map-side combiner stage and additional Reduce stages for each field with [DISTINCT](#distinct) aggregation are disabled.

### Example


```yql
SELECT
  key,
  COUNT(DISTINCT value) AS count -- топ-3 ключей по количеству уникальных значений
FROM my_table
GROUP /*+ COMPACT() */ BY key
ORDER BY count DESC
LIMIT 3;
```

{% endif %}

## GROUP BY ... HOP {#group-by-hop}

`HOP` groups data by overlapping time windows ( [hopping windows](https://en.wikipedia.org/wiki/Window_function_(SQL)#Hopping_window)). It is supported both in [analytical queries to tables](#hop-table) and in [streaming queries to topics](#hop-topic).


```yql
HOP(time_extractor, hop, interval, delay)
```


Where:

- `time_extractor` is an SQL expression of type `Timestamp` that defines the event time. A timestamp is computed from each input row, and this timestamp determines which window the row belongs to.
- `hop` — step between the starts of adjacent windows in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Durations) format, for example `"PT10S"` (10 seconds).
- `interval` — the size (duration) of each window in ISO 8601 format, for example `"PT30S"` (30 seconds).
- — window closing `delay` after its completion in ISO 8601 format. Used only in streaming queries (ignored when working with tables). For streaming queries, it is recommended to use [HoppingWindow](#group-by-hopping_window) with [watermarks](../../../../dev/streaming-query/watermarks.md) instead of `delay`.

The aggregate functions `HOP_START()` and `HOP_END()` are also available, which return the start and end timestamps of the current window of type `Timestamp`, respectively.

### Description {#hop-description}

Let's examine the algorithm using an example.


```yql
GROUP BY HOP(CAST(ts AS Timestamp), "PT10S", "PT30S", "PT20S")
```


In this example, `CAST(ts AS Timestamp)` extracts the event time from the `ts` column. The `hop` parameter is 10 seconds, `interval` is 30 seconds, and `delay` is 20 seconds.

Windows are built according to the following rule:

- Window starts are aligned to moments that are multiples of `hop` (10 seconds), starting from 0: 0, 10, 20, and so on.
- The duration of each window is `interval` (30 seconds). The resulting windows are: `[0; 30)`, `[10; 40)`, `[20; 50)`, and so on.
- An event falls into all windows whose time range includes its time. For example, an event with a time of 25 seconds falls into windows `[0; 30)`, `[10; 40)`, and `[20; 50)`.
- A window is considered complete when an event with a timestamp not less than the end of this window + `delay` (20 seconds) is received. For example, window `[10; 40)` closes when an event with a timestamp of 60 or more is received.

### Analytical `HOP` over a table {#hop-table}

When working with tables, data is grouped by `GROUP BY` keys (ignoring `HOP`), forming groups of rows (hereinafter groups). Within each group:

1. Rows are sorted in ascending order of `time_extractor`.
2. Each row is assigned to one or more overlapping windows.
3. The specified aggregate functions are computed on each window.

The `delay` parameter is **not used** in analytical table processing: the data is already fully available, the traversal order is determined by sorting by `time_extractor`, and the window completion is determined by the full group scan algorithm (see the closing rule in the [description](#hop-description) above).

### Streaming `HOP` over a topic {#hop-topic}

When working with [topics](../../../../concepts/datamodel/topic.md), data is grouped by `GROUP BY` keys (ignoring `HOP`), forming groups. Within each group:

1. Events are processed in an order close to ascending `time_extractor`. Minor deviations from the strict order are allowed.
2. Each event is assigned to one or more overlapping windows.
3. The specified aggregate functions are computed on each window.

In streaming queries, events may arrive not in strict chronological order. The `delay` parameter sets the wait time after the formal window completion: the window does not close immediately, but after `delay` seconds, to allow delayed events time to arrive. Events that arrive after the window closes are ignored.

### Limitations

`time_extractor` is an SQL expression that depends only on the input column values and must be of type `Timestamp`.

To specify `hop`, `interval`, and `delay`, a string expression conforming to the [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) standard is used, for example, `PT10S` — 10 seconds, `PT1M` — 1 minute. This is the format used to construct the built-in type `Interval` from a [string](../../builtins/basic.md#data-type-literals).

The values of parameters `interval` and `delay` must be divisible by the value of parameter `hop`. This requirement ensures window boundary alignment: each window starts at a time that is a multiple of `hop` and ends exactly after `interval`, guaranteeing uniform coverage of the time axis without gaps. Parameters `hop` and `interval` must be positive.

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

groups events by overlapping time windows (hopping windows), similar to [GROUP BY `HoppingWindow`](#group-by-hop). It is supported both in [analytical queries on tables](#hopping-window-table) and in [streaming queries on topics](#hopping-window-topic). The main difference from `HOP` is that in streaming queries, `HoppingWindow` uses the [watermarks](../../../../dev/streaming-query/watermarks.md) mechanism to determine when a window closes instead of the fixed `delay` parameter.


```yql
HoppingWindow(time_extractor, hop, interval)
```


Where:

- `time_extractor` — an SQL expression of type `Timestamp` that defines the event time. It must depend only on the input columns.
- `hop` — the step (shift period) between the starts of adjacent windows in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Durations) format, for example `"PT10S"` (10 seconds).
- `interval` — the size (duration) of each window in ISO 8601 format, for example `"PT1M"` (1 minute). The value of `interval` must be divisible by `hop`, because windows are aligned to multiples of the step interval.

The `HOP_START()` and `HOP_END()` functions are also available, returning the timestamps of the start and end of the current window.

The window construction algorithm is the same as [GROUP BY HOP](#group-by-hop): windows start at times that are multiples of `hop` and have a duration of `interval`. An event falls into all windows whose time range includes its time.

### Analytical `HoppingWindow` over a table {#hopping-window-table}

When working with tables, `HoppingWindow` performs grouping by time windows similarly to [HOP](#group-by-hop), but without the `delay` parameter, which is always ignored in analytical usage (the data in the table is already sorted).

1. The input table is partitioned by the grouping keys specified in `GROUP BY`, ignoring `HoppingWindow`.
2. Each partition is sorted by ascending `time_extractor`.
3. Each partition is divided into overlapping subsets of events (windows).
4. The specified aggregate functions are computed on each subset.

### Streaming `HoppingWindow` over a topic {#hopping-window-topic}

When working with [topics](../../../../concepts/datamodel/topic.md), `HoppingWindow` uses [watermarks](../../../../dev/streaming-query/watermarks.md) to determine when a window closes. A window closes when the watermark value is not less than the end of the window. This provides more accurate aggregation results compared to `HOP`, where the window closes after a fixed `delay`.

1. The input topic is partitioned by the grouping keys specified in `GROUP BY`, ignoring `HoppingWindow`.
2. In each partition, the window advances independently of the others.
3. Events are processed in an order close to ascending `time_extractor`. Minor reorderings of the input stream are allowed.
4. Each partition is divided into overlapping subsets of events (windows).
5. A window closes when a watermark with a value not less than the end of the window is received. After closing, the aggregation result is output.
6. Events that arrive after the window closes are not included in the results.

For `HoppingWindow` to work correctly in streaming mode, you must configure the watermark in the [WITH](with.md) section of the source. For more information, see: [{#T}](../../../../dev/streaming-query/watermarks.md#configuration).

### Example

Below is a streaming read from a topic: in `SELECT` it is convenient to output the window end via `HOP_END()`. For **tables** in an analytical query, `HOP_START()` or `HOP_END()` is more often used, depending on which window boundary you want to show in the result; the meaning of the windows is the same, only the selected timestamp differs.


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

Filtering the `SELECT` result set based on the results of computing [aggregate functions](../../builtins/aggregation.md). The syntax is similar to the [`WHERE`](where.md) clause.

### Example


```yql
SELECT
    key
FROM my_table
GROUP BY key
HAVING COUNT(value) > 100;
```
