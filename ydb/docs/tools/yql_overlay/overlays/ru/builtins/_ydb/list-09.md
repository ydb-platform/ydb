### Примеры

{% if feature_column_container_type %}

```yql
SELECT
    ListZip(list_column_1, list_column_2, list_column_3),
    ListZipAll(list_column_1, list_column_2)
FROM my_table;
```

{% endif %}

```yql
$l1 = AsList("a", "b");
$l2 = AsList(1, 2, 3);

SELECT ListZip($l1, $l2);  -- [("a",1),("b",2)]
SELECT ListZipAll($l1, $l2);  -- [("a",1),("b",2),(null,3)]
```

### Сигнатура
