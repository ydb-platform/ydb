### Примеры

{% if feature_column_container_type %}

```yql
SELECT
    ListIndexOf(list_column, 123)
FROM my_table;
```

{% endif %}

```yql
$l1 = AsList(1, 2, 3, 4, 5);

SELECT ListIndexOf($l1, 2);  -- 1
```

### Сигнатура
