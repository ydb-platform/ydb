### Examples

```yql
SELECT DictLookup(AsDict(
    AsTuple(1, AsList("foo", "bar")),
    AsTuple(2, AsList("bar", "baz"))
), 1);
-- [ "foo", "bar" ]
```

{% if feature_column_container_type %}

```yql
SELECT DictLookup(dict_column, "foo")
FROM my_table;
```

{% endif %}
