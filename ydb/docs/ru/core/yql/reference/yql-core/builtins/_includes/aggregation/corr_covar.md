## CORRELATION и COVARIANCE {#correlation-covariance}

**Сигнатура**
```
CORRELATION(Double?, Double?)->Double?
COVARIANCE(Double?, Double?)->Double?
COVARIANCE_SAMPLE(Double?, Double?)->Double?
COVARIANCE_POPULATION(Double?, Double?)->Double?
```

Корреляция и ковариация двух колонок.

Также доступны сокращенные версии `CORR` или `COVAR`, а для ковариации - версии с суффиксом `SAMPLE` / `POPULATION` по аналогии с описанной выше [VARIANCE](#variance).

В отличие от большинства других агрегатных функций не пропускают `NULL`, а считают его за 0.

При использовании [фабрики агрегационной функции](../../basic.md#aggregationfactory) в качестве первого аргумента [AGGREGATE_BY](#aggregateby) передается `Tuple` из двух значений.

**Примеры**
``` yql
SELECT
  CORRELATION(numeric_column, another_numeric_column),
  COVARIANCE(numeric_column, another_numeric_column)
FROM my_table;
```

``` yql
$corr_factory = AggregationFactory("CORRELATION");

SELECT
    AGGREGATE_BY(AsTuple(numeric_column, another_numeric_column), $corr_factory)
FROM my_table;
```