# WHERE

Filtering rows in the `SELECT`  result based on a condition in {% if backend_name == "YDB" %}[row-oriented](../../../../../concepts/datamodel/table.md#row-oriented-tables) or [column-oriented](../../../../../concepts/datamodel/table.md#column-oriented-tables){% else %} tables{% endif %}.

**Example**

```yql
SELECT key FROM my_table
WHERE value > 0;
```

