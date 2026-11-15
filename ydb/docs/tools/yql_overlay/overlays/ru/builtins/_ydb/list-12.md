### Примеры

{% if feature_column_container_type %}

```yql
SELECT
    ListSkip(list_column, 3)
FROM my_table;
```

{% endif %}

```yql
$l1 = AsList(1, 2, 3, 4, 5);

SELECT ListSkip($l1, 2);  -- [3,4,5]
```

### Сигнатура
