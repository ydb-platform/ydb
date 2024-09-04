## DISTINCT {#distinct}

Applying [aggregate functions](../../../builtins/aggregation.md) only to distinct values of the column.

{% note info %}

Applying `DISTINCT` to calculated values is not currently implemented. For this purpose, you can use a [subquery](../../select/from.md) or the expression `GROUP BY ... AS ...`.

{% endnote %}

**Example**

```sql
SELECT
  key,
  COUNT (DISTINCT value) AS count -- top 3 keys by the number of unique values
FROM my_table
GROUP BY key
ORDER BY count DESC
LIMIT 3;
```

You can also use `DISTINCT` to fetch distinct rows using [`SELECT DISTINCT`](../../select/distinct.md).

