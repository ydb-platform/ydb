## TOP и BOTTOM {#top-bottom}

**Сигнатура**
```
TOP(T?, limit:Uint32)->List<T>
TOP(T, limit:Uint32)->List<T>
BOTTOM(T?, limit:Uint32)->List<T>
BOTTOM(T, limit:Uint32)->List<T>
```

Вернуть список максимальных/минимальных значений выражения. Первый аргумент - выражение, второй - ограничение на количество элементов.

**Примеры**
``` yql
SELECT
    TOP(key, 3),
    BOTTOM(value, 3)
FROM my_table;
```

``` yql
$top_factory = AggregationFactory("TOP", 3);
$bottom_factory = AggregationFactory("BOTTOM", 3);

SELECT
    AGGREGATE_BY(key, $top_factory),
    AGGREGATE_BY(value, $bottom_factory)
FROM my_table;
```

## TOP_BY и BOTTOM_BY {#top-bottom-by}

**Сигнатура**
```
TOP_BY(T1, T2, limit:Uint32)->List<T1>
BOTTOM_BY(T1, T2, limit:Uint32)->List<T1>
```

Вернуть список значений первого аргумента для строк с максимальными/минимальными значениями второго аргумента. Третий аргумент - ограничение на количество элементов в списке.

При использовании [фабрики агрегационной функции](../../basic.md#aggregationfactory) в качестве первого аргумента [AGGREGATE_BY](#aggregateby) передается `Tuple` из значения и ключа. Ограничение на количество элементов в этом случае передаётся вторым аргументом при создании фабрики.

**Примеры**
``` yql
SELECT
    TOP_BY(value, LENGTH(value), 3),
    BOTTOM_BY(value, key, 3)
FROM my_table;
```

``` yql
$top_by_factory = AggregationFactory("TOP_BY", 3);
$bottom_by_factory = AggregationFactory("BOTTOM_BY", 3);

SELECT
    AGGREGATE_BY(AsTuple(value, LENGTH(value)), $top_by_factory),
    AGGREGATE_BY(AsTuple(value, key), $bottom_by_factory)
FROM my_table;
```
