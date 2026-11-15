### Examples

```yql
SELECT DictPayloads(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ [ "foo", "bar" ] ]
```

{% if feature_column_container_type %}

```yql
SELECT DictPayloads(dict_column)
FROM my_table;
```

{% endif %}
