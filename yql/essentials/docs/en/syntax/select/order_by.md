# ORDER BY

Sorting the `SELECT` result using a comma-separated list of sorting criteria. As a criteria, you can use a column value or an expression on columns. Ordering by column sequence number is not supported (`ORDER BY N` where `N` is a number).

Each criteria can be followed by the sorting direction:

- `ASC`: Sorting in the ascending order. Applied by default.
- `DESC`: Sorting in the descending order.

Multiple sorting criteria will be applied left-to-right.

{% note info %}

`NaN` is considered greater than all other key values, so it is sorted last in ascending order (`ASC`).

{% endnote %}

## Example

```yql
SELECT key, string_column
FROM my_table
ORDER BY key DESC, LENGTH(string_column) ASC;
```

You can also use `ORDER BY` for [window functions](../window.md).
