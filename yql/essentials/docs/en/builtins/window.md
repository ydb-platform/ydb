
# List of window functions in YQL

The syntax for calling window functions is detailed in a [separate article](../syntax/window.md).



## Aggregate functions {#aggregate-functions}

All [aggregate functions](aggregation.md) can also be used as window functions.
In this case, each row includes an aggregation result obtained on a set of rows from the [window frame](../syntax/window.md#frame).

#### Examples

```yql
SELECT
    SUM(int_column) OVER w1 AS running_total,
    SUM(int_column) OVER w2 AS total,
FROM my_table
WINDOW
    w1 AS (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    w2 AS ();
```



## ROW_NUMBER {#row_number}

Row number within a [partition](../syntax/window.md#partition). No arguments.

#### Signature

```yql
ROW_NUMBER()->Uint64
```

#### Examples

```yql
SELECT
    ROW_NUMBER() OVER w AS row_num
FROM my_table
WINDOW w AS (ORDER BY key);
```



## LAG / LEAD {#lag-lead}

Accessing a value from a row in the [section](../syntax/window.md#partition) that lags behind (`LAG`) or leads (`LEAD`) the current row by a fixed number. The first argument specifies the expression to be accessed, and the second argument specifies the offset in rows. You may omit the offset. By default, the neighbor row is used: the previous or next, respectively (hence, 1 is assumed by default). For the rows having no neighbors at a given distance (for example, `LAG(expr, 3)` `NULL` is returned in the first and second rows of the section).

#### Signature

```yql
LEAD(T[,Int32])->T?
LAG(T[,Int32])->T?
```

#### Examples

```yql
SELECT
   int_value - LAG(int_value) OVER w AS int_value_diff
FROM my_table
WINDOW w AS (ORDER BY key);
```

```yql
SELECT item, odd, LAG(item, 1) OVER w as lag1 FROM (
    SELECT item, item % 2 as odd FROM (
        SELECT AsList(1, 2, 3, 4, 5, 6, 7) as item
    )
    FLATTEN BY item
)
WINDOW w As (
    PARTITION BY odd
    ORDER BY item
);

/* Output:
item  odd  lag1
--------------------
2  0  NULL
4  0  2
6  0  4
1  1  NULL
3  1  1
5  1  3
7  1  5
*/
```


## FIRST_VALUE / LAST_VALUE {#first-last-value}

Access values from the first and last rows (using the `ORDER BY` clause for the window) of the [window frame](../syntax/window.md#frame). The only argument is the expression that you need to access.

Optionally, `OVER` can be preceded by the additional modifier `IGNORE NULLS`. It changes the behavior of functions to the first or last **non-empty** (i.e., non-`NULL`) value among the window frame rows. The antonym of this modifier is `RESPECT NULLS`: it's the default behavior that can be omitted.

#### Signature

```yql
FIRST_VALUE(T)->T?
LAST_VALUE(T)->T?
```

#### Examples

```yql
SELECT
   FIRST_VALUE(my_column) OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```

```yql
SELECT
   LAST_VALUE(my_column) IGNORE NULLS OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```



## NTH_VALUE

Access a value from a row specified by position in the window's `ORDER BY` order within [window frame](../syntax/window.md#frame). Arguments - the expression to access and the row number, starting with 1.

Optionally, the `IGNORE NULLS` modifier can be specified before `OVER`, which causes rows with `NULL` in the first argument's value to be skipped. The antonym of this modifier is `RESPECT NULLS`, which is the default behavior and may be skipped.

#### Signature

```yql
NTH_VALUE(T,N)->T?
```

#### Examples

```yql
SELECT
   NTH_VALUE(my_column, 2) OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```

```yql
SELECT
   NTH_VALUE(my_column, 3) IGNORE NULLS OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```




## RANK / DENSE_RANK / PERCENT_RANK {#rank}

Number the groups of neighboring rows in the [partition](../syntax/window.md#partition) with the same expression value in the argument. `DENSE_RANK` numbers the groups one by one, and `RANK` skips `(N - 1)` values, with `N` being the number of rows in the previous group. `PERCENT_RANK` returns the relative rank of the current row: $(RANK - 1)/(number of rows in the partition - 1)$.

If there is no argument, it uses the order specified in the `ORDER BY` section in the window definition.
If the argument is omitted and `ORDER BY` is not specified, then all rows are considered equal to each other.

{% note info %}

Passing an argument to `RANK`/`DENSE_RANK`/`PERCENT_RANK` is a non-standard extension in YQL.

{% endnote %}

#### Signature

```text
RANK([T])->Uint64
DENSE_RANK([T])->Uint64
PERCENT_RANK([T])->Double
```

#### Examples

```yql
SELECT
   RANK(my_column) OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```

```yql
SELECT
   DENSE_RANK() OVER w
FROM my_table
WINDOW w AS (ORDER BY my_column);
```

```yql
SELECT
   PERCENT_RANK() OVER w
FROM my_table
WINDOW w AS (ORDER BY my_column);
```



## NTILE

Distributes the rows of an ordered [partition](../syntax/window.md#partition) into a specified number of groups. The groups are numbered starting with one. For each row, the `NTILE` function returns the number of the group to which the row belongs.

#### Signature

```yql
NTILE(Uint64)->Uint64
```

#### Examples

```yql
SELECT
    NTILE(10) OVER w AS group_num
FROM my_table
WINDOW w AS (ORDER BY key);
```



## CUME_DIST

Returns the relative position (> 0 and <= 1) of a row within a [partition](../syntax/window.md#partition). No arguments.

#### Signature

```yql
CUME_DIST()->Double
```

#### Examples

```yql
SELECT
    CUME_DIST() OVER w AS dist
FROM my_table
WINDOW w AS (ORDER BY key);
```


## SessionState {#session-state}

A non-standard window function `SessionState()` (without arguments) lets you get the session calculation status from [SessionWindow](../syntax/group_by.md#session-window) for the current row.
It's allowed only if `SessionWindow()` is present in the `PARTITION BY` section in the window definition.

