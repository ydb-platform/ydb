# WHERE

Filtering rows in the `SELECT`  result based on a condition in {% if backend_name == "YDB" %}row-oriented or column-oriented{% else %} tables{% endif %}.

**Example**

```yql
SELECT key FROM my_table
WHERE value > 0;
```

