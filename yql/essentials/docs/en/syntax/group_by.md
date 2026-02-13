## GROUP BY

Group the `SELECT` results by the values of the specified columns or expressions. `GROUP BY` is often combined with [aggregate functions](../builtins/aggregation.md) (`COUNT`, `MAX`, `MIN`, `SUM`, `AVG`) to perform calculations in each group.

#### Syntax

```yql
SELECT                             -- In SELECT, you can use:
    column1,                       -- key columns specified in GROUP BY
    key_n,                         -- named expressions specified in GROUP BY
    column1 + key_n,               -- arbitrary non-aggregate functions on them
    Aggr_Func1( column2 ),         -- aggregate functions containing any columns as arguments,
    Aggr_Func2( key_n + column2 ), -- including named expressions specified in GROUP BY
    ...
FROM table
GROUP BY
    column1, column2, ...,
    <expr> AS key_n           -- When grouping by expression, you can set a name for it using AS,
                              -- and use it in SELECT
```

The query in the format `SELECT * FROM table GROUP BY k1, k2, ...` returns all columns listed in GROUP BY, i.e., is equivalent to `SELECT DISTINCT k1, k2, ... FROM table`.

An asterisk can also be used as an argument for the `COUNT` aggregate function. `COUNT(*)` means "the count of rows in the group".

{% note info %}

Aggregate functions ignore `NULL` in their arguments, except for `COUNT`.

{% endnote %}

YQL also provides aggregation factories implemented by the functions [`AGGREGATION_FACTORY`](../builtins/basic.md#aggregationfactory) and [`AGGREGATE_BY`](../builtins/aggregation.md#aggregateby).

#### Examples

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
   double_key,                           -- OK: A key column
   COUNT(*) AS group_size,               -- OK: COUNT(*)
   SUM(key + subkey) AS sum1,            -- OK: An aggregate function
   CAST(SUM(1 + 2) AS String) AS sum2,   -- OK: An aggregate function with a constant argument
   SUM(SUM(1) + key) AS sum3,            -- ERROR: Nested aggregations are not allowed
   key AS k1,                            -- ERROR: Using a non-key column named key without aggregation
   key * 2 AS dk1,                       -- ERROR in YQL: using a non-key column named key without aggregation
FROM my_table
GROUP BY
  key * 2 AS double_key,
  subkey as sk,
```

{% note warning %}

Specifying a name for a column or expression in `GROUP BY .. AS foo` it is an extension on top of YQL. 

Before language version [2025.02](../changelog/2025.02.md) such a name was visible in `WHERE` despite the fact that filtering by `WHERE` is executed [before](select/where.md) the grouping. For example, if the `T` table includes two columns, `foo` and `bar`, then the query `SELECT foo FROM T WHERE foo > 0 GROUP BY bar AS foo` would actually filter data by the `bar` column from the source table.

{% endnote %}



## GROUP BY ... SessionWindow() {#session-window}

YQL supports grouping by session. To standard expressions in `GROUP BY`, you can add a special `SessionWindow` function:

```yql
SELECT
  user,
  session_start,
  SessionStart() AS same_session_start, -- It's same as session_start
  COUNT(*) AS session_size,
  SUM(value) AS sum_over_session,
FROM my_table
GROUP BY user, SessionWindow(<time_expr>, <timeout_expr>) AS session_start
```

The following happens in this case:

1. The input table is partitioned by the grouping keys specified in `GROUP BY`, ignoring SessionWindow (in this case, it's based on `user`).

   If `GROUP BY` includes nothing more than SessionWindow, then the input table gets into one partition.

2. Each partition is split into disjoint subsets of rows (sessions).

   For this, the partition is sorted in the ascending order of the `time_expr` expression.
   The session limits are drawn between neighboring items of the partition, that differ in their `time_expr` values by more than `timeout_expr`.

3. The sessions obtained in this way are the final partitions on which aggregate functions are calculated.

The `SessionWindow()` key column (in the example, it's `session_start`) has the value "the minimum `time_expr` in the session".
If `GROUP BY` includes SessionWindow(), you can use a special aggregate function
[SessionStart](../builtins/aggregation.md#session-start).

An extended version of SessionWindow with four arguments is also supported:

`SessionWindow(<order_expr>, <init_lambda>, <update_lambda>, <calculate_lambda>)`

Where:

* `<order_expr>`: An expression used to sort the source partition
* `<init_lambda>`: A lambda function to initialize the state of session calculation. It has the signature `(TableRow())->State`. It's called once for the first (following the sorting order) element of the source partition
* `<update_lambda>`: A lambda function to update the status of session calculation and define the session limits. It has the signature `(TableRow(), State)->Tuple<Bool, State>`. It's called for every item of the source partition, except the first one. The new value of state is calculated based on the current row of the table and the previous state. If the first item in the return tuple is `True`, then a new session starts from the _current_ row. The key of the new session is obtained by applying `<calculate_lambda>` to the second item in the tuple.
* `<calculate_lambda>`: A lambda function for calculating the session key (the "value" of SessionWindow() that is also accessible via SessionStart()). The function has the signature `(TableRow(), State)->SessionKey`. It's called for the first item in the partition (after `<init_lambda>`) and those items for which `<update_lambda>` has returned `True` in the first item in the tuple. Please note that to start a new session, you should make sure that `<calculate_lambda>` has returned a value different from the previous session key. Sessions having the same keys are not merged. For example, if `<calculate_lambda>` returns the sequence `0, 1, 0, 1`, then there will be four different sessions.

Using the extended version of SessionWindow, you can, for example, do the following: divide a partition into sessions, as in the SessionWindow use case with two arguments, but with the maximum session length limited by a certain constant:

#### Example

```yql
$max_len = 1000; -- is the maximum session length.
$timeout = 100; -- is the timeout (timeout_expr in a simplified version of SessionWindow).

$init = ($row) -> (AsTuple($row.ts, $row.ts)); -- is the session status: tuple from 1) value of the temporary column ts in the session's first line and 2) in the current line
$update = ($row, $state) -> {
  $is_end_session = $row.ts - $state.0 > $max_len OR $row.ts - $state.1 > $timeout;
  $new_state = AsTuple(IF($is_end_session, $row.ts, $state.0), $row.ts);
  return AsTuple($is_end_session, $new_state);
};
$calculate = ($row, $state) -> ($row.ts);
SELECT
  user,
  session_start,
  SessionStart() AS same_session_start, -- It's same as session_start
  COUNT(*) AS session_size,
  SUM(value) AS sum_over_session,
FROM my_table
GROUP BY user, SessionWindow(ts, $init, $update, $calculate) AS session_start
```

You can use `SessionWindow` in `GROUP BY` only once.

## ROLLUP, CUBE, and GROUPING SETS {#rollup}

The results of calculating the aggregate function as subtotals for the groups and overall totals over individual columns or whole table.

#### Syntax

```yql
SELECT
    c1, c2, -- the columns to group by

AGGREGATE_FUNCTION(c3) AS outcome_c  -- an aggregate function (SUM, AVG, MIN, MAX, COUNT)

FROM table_name

GROUP BY
    GROUP_BY_EXTENSION(c1, c2)       -- an extension of GROUP BY: ROLLUP, CUBE, or GROUPING SETS
```

* `ROLLUP` groups the column values in the order they are listed in the arguments (strictly from left to right), generates subtotals for each group and the overall total.
* `CUBE` groups the values for every possible combination of columns, generates the subtotals for each group and the overall total.
* `GROUPING SETS` sets the groups for subtotals.

You can combine `ROLLUP`, `CUBE` and `GROUPING SETS`, separating them by commas.

### GROUPING {#grouping}

The values of columns not used in calculations are replaced with `NULL` in the subtotal. In the overall total, the values of all columns are replaced by `NULL`. `GROUPING`: A function that allows you to distinguish the source `NULL` values from the `NULL` values added while calculating subtotals and overall totals.

`GROUPING` returns a bit mask:

* `0`: If `NULL` is used for the original empty value.
* `1`: If `NULL` is added for a subtotal or overall total.

#### Example

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
        -- if you add here (column2) as well, then together
        -- the ROLLUP and GROUPING SETS would produce a result
        -- similar to CUBE
    )
;
```

## DISTINCT {#distinct}

Applying [aggregate functions](../builtins/aggregation.md) only to distinct values of the column.

{% note info %}

Applying `DISTINCT` to calculated values is not currently implemented. For this purpose, you can use a [subquery](select/from.md) or the expression `GROUP BY ... AS ...`.

{% endnote %}

#### Example

```yql
SELECT
  key,
  COUNT (DISTINCT value) AS count -- top 3 keys by the number of unique values
FROM my_table
GROUP BY key
ORDER BY count DESC
LIMIT 3;
```

You can also use `DISTINCT` to fetch distinct rows using [`SELECT DISTINCT`](select/distinct.md).

## GROUP COMPACT BY

Improves aggregation efficiency if the query author knows in advance that none of aggregation keys finds large amounts of data (i.e., with the order of magnitude exceeding a gigabyte or a million of rows). If this assumption fails to materialize, then the operation may fail with Out of Memory error or start running much slower compared to the non-COMPACT version.

Unlike the usual GROUP BY, the Map-side combiner stage and additional Reduce are disabled for each field with [DISTINCT](../syntax/group_by.md#distinct) aggregation.

#### Example

```yql
SELECT
  key,
  COUNT (DISTINCT value) AS count -- top 3 keys by the number of unique values
FROM my_table
GROUP COMPACT BY key
ORDER BY count DESC
LIMIT 3;
```

## HAVING {#having}

Filtering a `SELECT` based on the calculation results of [aggregate functions](../builtins/aggregation.md). The syntax is similar to [WHERE](select/where.md).

#### Example

```yql
SELECT
    key
FROM my_table
GROUP BY key
HAVING COUNT(value) > 100;
```
