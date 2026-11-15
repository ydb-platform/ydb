### Examples

```yql
SELECT DictKeys(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ 1 ]
```

{% if feature_column_container_type %}

```yql
SELECT DictKeys(dict_column)
FROM my_table;
```

{% endif %}
