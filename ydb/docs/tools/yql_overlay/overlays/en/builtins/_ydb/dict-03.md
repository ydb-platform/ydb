### Examples

```yql
SELECT DictLength(AsDict(AsTuple(1, AsList("foo", "bar"))));
```

{% if feature_column_container_type %}

```yql
SELECT DictLength(dict_column) FROM my_table;
```

{% endif %}
