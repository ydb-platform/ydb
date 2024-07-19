## RANK / DENSE_RANK / PERCENT_RANK {#rank}

Number the groups of neighboring rows in the [partition](../../../syntax/window.md#partition) with the same expression value in the argument. `DENSE_RANK` numbers the groups one by one, and `RANK` skips `(N - 1)` values, with `N` being the number of rows in the previous group. `PERCENT_RANK` returns the relative rank of the current row: `(RANK - 1)/(number of rows in the partition - 1)`.

If there is no argument, it uses the order specified in the `ORDER BY` section in the window definition.
If the argument is omitted and `ORDER BY` is not specified, then all rows are considered equal to each other.

{% note info %}

Passing an argument to `RANK`/`DENSE_RANK`/`PERCENT_RANK` is a non-standard extension in YQL.

{% endnote %}

**Signature**

```
RANK([T])->Uint64
DENSE_RANK([T])->Uint64
PERCENT_RANK([T])->Double
```

**Examples**

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

