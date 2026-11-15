### Примеры

{% if feature_column_container_type %}

```yql
SELECT
    ListHas(list_column, "my_needle")
FROM my_table;
```

{% endif %}

```yql
$l1 = AsList(1, 2, 3, 4, 5);

SELECT ListHas($l1, 2);  -- true
SELECT ListHas($l1, 6);  -- false
```

### Сигнатура
