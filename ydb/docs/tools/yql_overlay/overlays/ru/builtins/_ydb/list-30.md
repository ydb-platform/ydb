### Примеры

{% if feature_column_container_type %}

```yql
SELECT
    ListExtract(struct_list_column, "MyMember")
FROM my_table;
```

{% endif %}

```yql
$l = AsList(
    <|key:"a", value:1|>,
    <|key:"b", value:2|>
);
SELECT ListExtract($l, "key");  -- ["a", "b"]
```

### Сигнатура
