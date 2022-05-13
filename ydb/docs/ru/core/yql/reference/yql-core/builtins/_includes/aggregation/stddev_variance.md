## STDDEV и VARIANCE {#stddev-variance}

**Сигнатура**
```
STDDEV(Double?)->Double?
STDDEV_POPULATION(Double?)->Double?
POPULATION_STDDEV(Double?)->Double?
STDDEV_SAMPLE(Double?)->Double?
STDDEVSAMP(Double?)->Double?

VARIANCE(Double?)->Double?
VARIANCE_POPULATION(Double?)->Double?
POPULATION_VARIANCE(Double?)->Double?
VARPOP(Double?)->Double?
VARIANCE_SAMPLE(Double?)->Double?
```

Стандартное отклонение и дисперсия по колонке. Используется [однопроходной параллельный алгоритм](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm), результат которого может отличаться от полученного более распространенными методами, требующими двух проходов по данным.

По умолчанию вычисляются выборочная дисперсия и стандартное отклонение. Доступны несколько способов записи:

* с суффиксом/префиксом `POPULATION`, например: `VARIANCE_POPULATION`, `POPULATION_VARIANCE` — вычисляет дисперсию/стандартное отклонение для генеральной совокупности;
* с суффиксом `SAMPLE` или без суффикса, например `VARIANCE_SAMPLE`, `SAMPLE_VARIANCE`, `VARIANCE` — вычисляет выборочную дисперсию и стандартное отклонение.

Также определено несколько сокращенных алиасов, например `VARPOP` или `STDDEVSAMP`.

Если все переданные значения — `NULL`, возвращает `NULL`.

**Примеры**
``` yql
SELECT
  STDDEV(numeric_column),
  VARIANCE(numeric_column)
FROM my_table;
```
