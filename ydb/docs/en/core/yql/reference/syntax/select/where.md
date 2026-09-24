# WHERE

Filtering rows in the `SELECT`  result based on a condition in {% if backend_name == "YDB" %}[row-oriented](../../../../concepts/datamodel/table.md#row-oriented-tables) or [column-oriented](../../../../concepts/datamodel/table.md#column-oriented-tables){% else %} tables{% endif %}.

## Example

```yql
SELECT key FROM my_table
WHERE value > 0;
```

{% note warning %}

A subquery in `WHERE` cannot refer to columns or table aliases from an outer
query. To select rows that have matching rows in another table, use
[`LEFT SEMI JOIN`](../correlated-subqueries.md#exists). To select rows without
matches, use [`LEFT ONLY JOIN`](../correlated-subqueries.md#not-exists).

{% endnote %}
