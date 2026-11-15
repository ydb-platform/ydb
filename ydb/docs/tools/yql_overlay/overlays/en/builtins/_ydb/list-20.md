Otherwise, it returns false.

{% if feature_column_container_type %}

### Examples

```yql
SELECT
    ListAll(bool_column),
    ListAny(bool_column)
FROM my_table;
```

{% endif %}
