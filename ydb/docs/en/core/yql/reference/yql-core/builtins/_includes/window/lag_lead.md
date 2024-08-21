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

```yql
SELECT l, x, f, LAG(l, 1) OVER w as ll1 FROM (
SELECT l, l*l as f, 1 as x FROM(
  SELECT AsList(1,2,3,4,5) as l
)
FLATTEN BY l
)
WINDOW w As (
    PARTITION BY x
    ORDER BY f
);

/* Output:
l	x	f	ll1
-------------
1	1	1	NULL
2	1	4	1
3	1	9	2
4	1	16	3
5	1	25	4
*/

```
