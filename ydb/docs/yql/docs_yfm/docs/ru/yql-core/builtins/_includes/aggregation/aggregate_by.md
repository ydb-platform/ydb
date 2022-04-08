
## AGGREGATE_BY и MULTI_AGGREGATE_BY {#aggregate-by}
Применение [фабрики агрегационной функции](../../basic.md#aggregationfactory) ко всем значениям колонки или выражения. Функция `MULTI_AGGREGATE_BY` требует, чтобы в значении колонки или выражения была структура, кортеж или список, и применяет фабрику поэлементно, размещая результат в контейнере той же формы. Если в разных значениях колонки или выражения содержатся списки разной длины, результирующий список будет иметь наименьшую из длин этих списков.

1. Колонка, `DISTINCT` колонка или выражение;
2. Фабрика.

**Примеры:**
``` yql
$count_factory = AggregationFactory("COUNT");

SELECT
    AGGREGATE_BY(DISTINCT column, $count_factory) as uniq_count
FROM my_table;

SELECT
    MULTI_AGGREGATE_BY(nums, AggregationFactory("count")) as count,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("min")) as min,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("max")) as max,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("avg")) as avg,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("percentile", 0.9)) as p90
FROM my_table;
```
