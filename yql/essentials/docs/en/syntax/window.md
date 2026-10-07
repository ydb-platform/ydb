# OVER, PARTITION BY, and WINDOW

Window functions were introduced in the SQL:2003 standard and expanded in the SQL:2011 standard. They let you run calculations on a set of table rows that are related to the current row in some way.

Unlike [aggregate functions](../builtins/aggregation.md), window functions don't group rows into one output row: the number of rows in the resulting table is always the same as in the source table.

If a query contains both aggregate and window functions, grouping is performed and aggregate function values are calculated first. The calculated values of aggregate functions can be used as window function arguments (but not the other way around).

## Syntax {#syntax}

General syntax for calling a window function is as follows

```yql
function_name([expression [, expression ...]]) OVER (window_definition)
```

or

```yql
function_name([expression [, expression ...]]) OVER window_name
```

Here, window name (`window_name`) is an arbitrary ID that is unique within the query and `expression` is an arbitrary expression that contains no window function calls.

In the query, each window name must be mapped to the window definition (`window_definition`):

```yql
SELECT
    F0(...) OVER (window_definition_0),
    F1(...) OVER w1,
    F2(...) OVER w2,
    ...
FROM my_table
WINDOW
    w1 AS (window_definition_1),
    ...
    w2 AS (window_definition_2)
;
```

Here, the `window_definition` is written as

```yql
[ PARTITION BY (expression AS column_identifier | column_identifier) [, ...] ]
[ ORDER BY expression [ASC | DESC] ]
[ frame_definition ]
```

You can set an optional *frame definition* (`frame_definition`) in one of the following ways:

* `ROWS frame_begin`
* `ROWS BETWEEN frame_begin AND frame_end`
* `RANGE frame_begin`
* `RANGE BETWEEN frame_begin AND frame_end`

{% note info %}

`RANGE` mode is available starting from language version 2026.01.

{% endnote %}

*The frame start* (`frame_begin`) and *frame end* (`frame_end`) are set in one of the following ways:

* `UNBOUNDED PRECEDING`
* `offset PRECEDING`
* `CURRENT ROW`
* `offset FOLLOWING`
* `UNBOUNDED FOLLOWING`

Here, the *frame* `offset` is a non-negative literal. If the frame end isn't set, `CURRENT ROW` is assumed.

In `ROWS` mode the offset is always an integer literal. In `RANGE` mode the offset type is determined by the `ORDER BY` column type and must support addition, subtraction, and comparison with it. The following `ORDER BY` column types are supported:

* All YQL numeric types: `Int8`, `Uint8`, `Int16`, `Uint16`, `Int32`, `Uint32`, `Int64`, `Uint64`, `Float`, `Double`, `Decimal` — offset must be one of these types (not necessarily the same one). You cannot use `NaN` and `Inf` values for floating point types.
* YQL date and time types: `Date`, `Datetime`, `Timestamp`, `TzDate`, `TzDatetime`, `TzTimestamp`, `Interval` — offset of type `Interval` or `Interval64`.
* Wide YQL date and time types: `Date32`, `Datetime64`, `Timestamp64`, `TzDate32`, `TzDatetime64`, `TzTimestamp64`, `Interval64` — offset of type `Interval` or `Interval64`.
* PostgreSQL types are supported in the same way.

There should be no window function calls in any of the expressions inside the window definition.

## Calculation algorithm

### Partitioning {#partition}

If `PARTITION BY` is set, the source table rows are grouped into *partitions*, which are then handled independently of each other.
If `PARTITION BY` isn't set, all rows in the source table are put in the same partition. If `ORDER BY` is set, it determines the order of rows in a partition.
Both in `PARTITION BY` and [GROUP BY](group_by.md) you can use aliases and [SessionWindow](group_by.md#session-window).

If `ORDER BY` is omitted, the order of rows in the partition is undefined.

### Frame {#frame}

The `frame_definition` specifies a set of partition rows that fall into the *window frame* associated with the current row.

In `ROWS` mode, the window frame contains rows with the specified offsets relative to the current row in the partition. For example, if `ROWS BETWEEN 3 PRECEDING AND 5 FOLLOWING` is used, the window frame contains 3 rows preceding the current one, the current row, and 5 rows following it.

In `RANGE` mode, the offset is defined not by a row count but by a value range of the `ORDER BY` column. To use `RANGE` mode with an offset (`offset PRECEDING` or `offset FOLLOWING`), `ORDER BY` must contain exactly one column of a supported type (see the `offset` description in the syntax section above). For example, with `RANGE BETWEEN 3 PRECEDING AND 5 FOLLOWING`, the window frame contains all partition rows whose `ORDER BY` column value is at most 3 less than and at most 5 greater than the current row's value. `RANGE CURRENT ROW` includes all rows with the same `ORDER BY` value as the current row. `UNBOUNDED PRECEDING` and `UNBOUNDED FOLLOWING` have the same meaning in `RANGE` mode as in `ROWS` mode.

The set of rows in the window frame may change depending on which row is the current one. For example, for the first row in the partition, the `ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING` window frame will have no rows.

Setting `UNBOUNDED PRECEDING` as the frame start means "from the first partition row" and `UNBOUNDED FOLLOWING` as the frame end — "up to the last partition row". Setting `CURRENT ROW` means "from/to the current row".

If no `frame_definition` is specified, a set of rows to be included in the window frame depends on whether there is `ORDER BY` in the `window_definition`.
Namely, if there is `ORDER BY`, then `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is implicitly assumed. If none, then `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

Further, depending on the specific window function, it's calculated either based on the set of rows in the partition or the set of rows in the window frame.

[List of available window functions](../builtins/window.md)

#### Examples

```yql
SELECT
    COUNT(*) OVER w AS rows_count_in_window,
    some_other_value -- access the current row
FROM `my_table`
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY int_column
);
```

```yql
SELECT
    LAG(my_column, 2) OVER w AS row_before_previous_one
FROM `my_table`
WINDOW w AS (
    PARTITION BY partition_key_column
);
```

```yql
SELECT
    -- AVG (like all aggregate functions used as window functions)
    -- is calculated on the window frame
    AVG(some_value) OVER w AS avg_of_prev_current_next,
    some_other_value -- access the current row
FROM my_table
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY int_column
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
);
```

```yql
SELECT
    -- LAG doesn't depend on the window frame position
    LAG(my_column, 2) OVER w AS row_before_previous_one
FROM my_table
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY my_column
);
```

```yql
-- RANGE CURRENT ROW: aggregate is calculated over all rows with the same
-- order_column value as the current row (including the current row itself)
SELECT
    order_column,
    SUM(amount) OVER w AS sum_for_same_order_value
FROM my_table
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY order_column
    RANGE CURRENT ROW
);
```

```yql
-- RANGE with a numeric offset: moving average over values within 10 units
-- of the current value in either direction
SELECT
    ts,
    AVG(value) OVER w AS moving_avg
FROM my_table
WINDOW w AS (
    ORDER BY ts
    RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING
);
```

```yql
-- RANGE UNBOUNDED: cumulative sum that includes all rows with the same
-- order_column value as the current row (not just strictly preceding ones)
SELECT
    order_column,
    SUM(amount) OVER w AS cumulative_sum
FROM my_table
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY order_column
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);
```

## Implementation specifics

* Functions calculated on the `ROWS/RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` or `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` window frame are implemented efficiently (do not require additional memory and their computation runs on a partition in O(partition size) time). `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` still requires additional memory to buffer rows that share the current row's `ORDER BY` value.

* For the `ROWS/RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` window frame, you can choose the execution strategy in RAM by specifying the `COMPACT` hint after the `PARTITION` keyword.

  For example, `PARTITION COMPACT BY key` or `PARTITION COMPACT BY ()` (if `PARTITION BY` was missing initially).

  If the `COMPACT` hint is specified, this requires additional memory equal to O(partition size), but then no extra `JOIN` operation is made.

* If the window frame doesn't start with `UNBOUNDED PRECEDING`, calculating window functions on this window requires additional memory equal to O(the maximum number of rows from the window boundaries to the current row), while the computation time is equal to O(number_of_partition_rows * window_size).
* For the window frame starting with `UNBOUNDED PRECEDING` and ending with `N`, where `N` is neither `CURRENT ROW` nor `UNBOUNDED FOLLOWING`, additional memory equal to O(N) is required and the computation time is equal to O(partition size).
* The `LEAD(expr, N)` and `LAG(expr, N)` functions always require O(N) of RAM.

Given the above, a query with `ROWS/RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` should, if possible, be changed to `ROWS/RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` by reversing the `ORDER BY` sorting order.
