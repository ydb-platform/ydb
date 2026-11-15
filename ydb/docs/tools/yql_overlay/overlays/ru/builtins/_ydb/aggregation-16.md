### Сигнатура

```yql
CORRELATION(Double?, Double?)->Double?
COVARIANCE(Double?, Double?)->Double?
COVARIANCE_SAMPLE(Double?, Double?)->Double?
COVARIANCE_POPULATION(Double?, Double?)->Double?
```

Корреляция и ковариация двух колонок.

Также доступны сокращенные версии `CORR` или `COVAR`, а для ковариации - версии с суффиксом `SAMPLE` / `POPULATION` по аналогии с описанной выше [VARIANCE](#stddev-variance).

В отличие от большинства других агрегатных функций не пропускают `NULL`, а считают его за 0.

При использовании [фабрики агрегационной функции](../basic.md#aggregationfactory) в качестве первого аргумента [AGGREGATE_BY](#aggregate-by) передается `Tuple` из двух значений.

### Примеры
