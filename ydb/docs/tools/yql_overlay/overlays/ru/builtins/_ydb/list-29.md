### Примеры

{% if feature_column_container_type %}

```yql
SELECT
    ListConcat(string_list_column),
    ListConcat(string_list_column, "; ")
FROM my_table;
```

{% endif %}

```yql
$l1 = AsList("h", "e", "l", "l", "o");

SELECT ListConcat($l1);  -- "hello"
SELECT ListConcat($l1, " ");  -- "h e l l o"
```

### Сигнатура
