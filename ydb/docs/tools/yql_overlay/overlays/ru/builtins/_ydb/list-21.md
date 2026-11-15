В противном случае возвращает false.

{% if feature_column_container_type %}

### Примеры

```yql
SELECT
    ListAll(bool_column),
    ListAny(bool_column)
FROM my_table;
```
{% endif %}

### Сигнатура
