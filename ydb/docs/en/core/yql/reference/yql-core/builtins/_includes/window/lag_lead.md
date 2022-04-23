## LAG / LEAD {#lag-lead}

Accessing a value from a row in the [section](../../../syntax/window.md#partition) that lags behind (`LAG`) or leads (`LEAD`) the current row by a fixed number. The first argument specifies the expression to be accessed, and the second argument specifies the offset in rows. You may omit the offset. By default, the neighbor row is used: the previous or next, respectively (hence, 1 is assumed by default). For the rows having no neighbors at a given distance (for example `LAG(expr, 3)` `NULL` is returned in the first and second rows of the section.

**Signature**

```
LEAD(T[,Int32])->T?
LAG(T[,Int32])->T?
```

**Examples**

```yql
SELECT
   int_value - LAG(int_value) OVER w AS int_value_diff
FROM my_table
WINDOW w AS (ORDER BY key);
```

