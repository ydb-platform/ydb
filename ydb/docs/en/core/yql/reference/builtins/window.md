# List of window functions in YQL

The syntax for calling window functions is detailed in a [separate article](../syntax/select/window.md).

{% if feature_window_functions %}

Window functions allow you to perform calculations on a set of rows related to the current row. Unlike aggregate functions, window functions don't group rows into a single output row: each row retains its individual identity.

In this case, each row includes an aggregation result obtained on a set of rows from the [window frame](../syntax/select/window.md#frame).

{% note info %}

Window functions can only be used in `SELECT` and `ORDER BY`.

{% endnote %}

## ROW_NUMBER

Row number within a [partition](../syntax/select/window.md#partition). No arguments.

### Examples

```yql
SELECT
    ROW_NUMBER() OVER w AS row_number,
    value
FROM my_table
WINDOW w AS (ORDER BY key);
```

## LAG / LEAD {#lag-lead}

Accessing a value from a row in the [section](../syntax/select/window.md#partition) that lags behind (`LAG`) or leads (`LEAD`) the current row by a fixed number. The first argument specifies the expression to be accessed, and the second argument specifies the offset in rows. You can also specify the third argument that will be used as the default value if the target row is outside the section (otherwise, `NULL` is used).

### Examples

```yql
SELECT
   int_value,
   LAG(int_value, 1) OVER w AS int_value_lag_1,
   LEAD(int_value, 2) OVER w AS int_value_lead_2,
FROM my_table
WINDOW w AS (ORDER BY key);
```

## FIRST_VALUE / LAST_VALUE {#first-last-value}

Access values from the first and last rows (using the `ORDER BY` clause for the window) of the [window frame](../syntax/select/window.md#frame). The only argument is the expression that you need to access.

### Examples

```yql
SELECT
   int_value,
   FIRST_VALUE(int_value) OVER w AS int_value_first,
   LAST_VALUE(int_value) OVER w AS int_value_last,
FROM my_table
WINDOW w AS (ORDER BY key);
```

## NTH_VALUE

Access a value from a row specified by position in the window's `ORDER BY` order within [window frame](../syntax/select/window.md#frame). Arguments - the expression to access and the row number, starting with 1.

### Examples

```yql
SELECT
   int_value,
   NTH_VALUE(int_value, 2) OVER w AS int_value_nth_2,
FROM my_table
WINDOW w AS (ORDER BY key);
```

## RANK / DENSE_RANK / PERCENT_RANK {#rank}

Number the groups of neighboring rows in the [partition](../syntax/select/window.md#partition) with the same expression value in the argument. `DENSE_RANK` numbers the groups one by one, and `RANK` skips `(N - 1)` values, with `N` being the number of rows in the previous group. `PERCENT_RANK` returns the relative rank of the current row: `(RANK - 1) / (total number of partition rows - 1)`. If the partition contains only one row, `PERCENT_RANK` returns 0.

If there's no argument, the numbering is based on the order specified in the window's `ORDER BY` clause.

### Examples

```yql
SELECT
   int_value,
   RANK(int_value) OVER w AS int_value_rank,
   DENSE_RANK() OVER w AS dense_rank,
   PERCENT_RANK() OVER w AS percent_rank,
FROM my_table
WINDOW w AS (ORDER BY key);
```

## NTILE

Distributes the rows of an ordered [partition](../syntax/select/window.md#partition) into a specified number of groups. The groups are numbered starting with one. For each row, the `NTILE` function returns the number of the group to which the row belongs.

### Examples

```yql
SELECT
   int_value,
   NTILE(3) OVER w AS int_value_ntile_3,
FROM my_table
WINDOW w AS (ORDER BY key);
```

## CUME_DIST

Returns the relative position (> 0 and <= 1) of a row within a [partition](../syntax/select/window.md#partition). No arguments.

### Examples

```yql
SELECT
   int_value,
   CUME_DIST() OVER w AS int_value_cume_dist,
FROM my_table
WINDOW w AS (ORDER BY key);
```

{% endif %}



## Aggregate functions {#aggregate-functions}

All [aggregate functions](aggregation.md) can also be used as window functions.
In this case, each row includes an aggregation result obtained on a set of rows from the [window frame](../syntax/select/window.md#frame).

### Examples

```yql
SELECT
    SUM(int_column) OVER w1 AS running_total,
    SUM(int_column) OVER w2 AS total,
FROM my_table
WINDOW
    w1 AS (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    w2 AS ();
```



## SessionState() {#session-state}

A non-standard window function `SessionState()` (without arguments) lets you get the session calculation status from [SessionWindow](../syntax/select/group-by.md#session-window) for the current row.
It's allowed only if `SessionWindow()` is present in the `PARTITION BY` section in the window definition.

