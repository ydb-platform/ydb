## RANK / DENSE_RANK {#rank}

Number the groups of neighboring rows in the [section](../../../syntax/window.md#partition) that have the same expression value in the argument. `DENSE_RANK` numbers the groups one-by-one, and `RANK` skips `(N - 1)` values, with `N` being the number of rows in the previous group.

If there is no argument, it uses the order specified in the `ORDER BY` section in the window definition.
If the argument is omitted and `ORDER BY` is not specified, then all rows are considered equal to each other.

{% note info %}

Passing an argument to `RANK`/`DENSE_RANK` is a non-standard extension in YQL.

{% endnote %}

**Signature**

```
RANK([T])->Uint64
DENSE_RANK([T])->Uint64
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
   RANK() OVER w
FROM my_table
WINDOW w AS (ORDER BY my_column);


