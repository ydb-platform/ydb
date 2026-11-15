{% if feature_column_container_type %}

### Examples

```yql
SELECT
    ListExtract(struct_list_column, "MyMember")
FROM my_table;
```

{% endif %}
