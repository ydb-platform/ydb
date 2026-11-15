### Примеры

{% if feature_column_container_type %}

```yql
SELECT ListSortDesc(list_column) FROM my_table;
```

{% endif %}

```yql
$list = AsList(
    AsTuple("x", 3),
    AsTuple("xx", 1),
    AsTuple("a", 2)
);

SELECT ListSort($list, ($x) -> {
    RETURN $x.1;
});
```

{% note info %}

В примере использовалась [лямбда функция](../../syntax/expressions.md#lambda).

{% endnote %}

### Сигнатура
