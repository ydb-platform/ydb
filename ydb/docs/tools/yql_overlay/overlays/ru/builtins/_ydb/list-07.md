{% if feature_column_container_type %}

### Примеры

```yql
SELECT ListExtend(
    list_column_1,
    list_column_2,
    list_column_3
) FROM my_table;
```

{% endif %}

```yql
$l1 = AsList("a", "b");
$l2 = AsList("b", "c");
$l3 = AsList("d", "e");

SELECT ListExtend($l1, $l2, $l3);  -- ["a","b","b","c","d","e"]
```

### Сигнатура
