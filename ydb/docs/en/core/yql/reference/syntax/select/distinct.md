# DISTINCT

Selecting unique rows.

{% note info %}

Applying `DISTINCT` to calculated values is not currently implemented. For this purpose, use a subquery or the clause [`GROUP BY ... AS ...`](group-by.md).

{% endnote %}

## Example

```yql
SELECT DISTINCT value -- only unique values from the table
FROM my_table;
```

The `DISTINCT` keyword can also be used to apply [aggregate functions](../../builtins/aggregation.md) only to distinct values. For more information, see the documentation for [GROUP BY](group-by.md).

Removes duplicate rows from the result. Applies after the clause [`GROUP BY ... AS ...`](group-by.md).

