### Примеры

{% if feature_column_container_type %}

```yql
SELECT
    ToSet(list_column)
FROM my_table;
```

{% endif %}

```yql
$l = AsList(1,1,2,2,3);
SELECT ToSet($l);  -- {1,2,3}
```

### Сигнатура
