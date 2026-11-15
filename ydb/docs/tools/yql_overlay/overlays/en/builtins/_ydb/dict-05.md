### Examples

```yql
SELECT DictItems(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ ( 1, [ "foo", "bar" ] ) ]
```

{% if feature_column_container_type %}

```yql
SELECT DictItems(dict_column)
FROM my_table;
```

{% endif %}
