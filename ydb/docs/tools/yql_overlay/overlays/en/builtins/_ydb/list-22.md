{% if feature_column_container_type %}

### Examples

```yql
SELECT
    ListHead(numeric_list_column) AS head,
    ListLast(numeric_list_column) AS last
FROM my_table;
```

{% endif %}
