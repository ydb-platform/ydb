### Примеры

{% if feature_column_container_type %}

```yql
SELECT ListUnionAll(
    list_column_1,
    list_column_2,
    list_column_3
) FROM my_table;
```

{% endif %}

```yql
$l1 = AsList(
    <|value:1|>,
    <|value:2|>
);
$l2 = AsList(
    <|key:"a"|>,
    <|key:"b"|>
);
SELECT ListUnionAll($l1, $l2);  -- result: [("value":1),("value":2),("key":"a"),("key":"b")]
                                -- schema: List<Struct<key : String?, value : Int32?>>
```

### Сигнатура
