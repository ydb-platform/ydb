{% if feature_column_container_type %}

### Examples

```yql
SELECT
    ListConcat(string_list_column),
    ListConcat(string_list_column, "; ")
FROM my_table;
```

{% endif %}
