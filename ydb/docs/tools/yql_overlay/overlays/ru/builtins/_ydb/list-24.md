{% if feature_column_container_type %}

### Примеры

```yql
SELECT
    ListMax(numeric_list_column) AS max,
    ListMin(numeric_list_column) AS min,
    ListSum(numeric_list_column) AS sum,
    ListAvg(numeric_list_column) AS avg
FROM my_table;
```
{% endif %}

### Сигнатура
