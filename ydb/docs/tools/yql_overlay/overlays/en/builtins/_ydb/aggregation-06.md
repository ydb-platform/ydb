### Signature


```yql
COUNT_IF(Bool?)->Uint64?
```


Number of rows for which the expression specified as the argument is true (the expression's calculation result is true).

The value `NULL` is equated to `false` (if the argument type is `Bool?`).

The function *does not* do the implicit type casting to Boolean for strings and numbers.

### Examples


```yql
SELECT
  COUNT_IF(value % 2 == 1) AS odd_count
FROM my_table;
```

{% note info %}

To count distinct values in rows meeting the condition, unlike other aggregate functions, you can't use the modifier [DISTINCT](../../syntax/select/group-by.md#distinct) because arguments contain no values. To get such a result, use a query like this:


```yql
SELECT
    COUNT(DISTINCT IF(value % 2 == 1, value))
FROM my_table;
```

{% endnote %}
