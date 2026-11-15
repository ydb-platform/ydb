### Примеры

{% if feature_column_container_type %}

```yql
SELECT
    ToDict(tuple_list_column)
FROM my_table;
```

{% endif %}

```yql
$l = AsList(("a",1), ("b", 2), ("a", 3));
SELECT ToDict($l);  -- {"a": 1,"b": 2}
```

### Сигнатура
