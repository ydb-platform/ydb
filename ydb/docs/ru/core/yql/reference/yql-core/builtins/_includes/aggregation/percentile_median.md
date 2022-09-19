## PERCENTILE и MEDIAN {#percentile-median}

**Сигнатура**
```
PERCENTILE(Double?, Double)->Double?
PERCENTILE(Interval?, Double)->Interval?

MEDIAN(Double? [, Double])->Double?
MEDIAN(Interval? [, Double])->Interval?
```

Подсчет процентилей по амортизированной версии алгоритма [TDigest](https://github.com/tdunning/t-digest). `MEDIAN` — алиас для `PERCENTILE(N, 0.5)`.

{% note info "Ограничение" %}

Первый аргумент (N) должен быть именем колонки таблицы. Если это ограничение необходимо обойти, можно использовать подзапрос. Ограничение введено для упрощения вычислений, поскольку в реализации несколько вызовов с одинаковым первым аргументом (N) склеиваются в один проход.

{% endnote %}

``` yql
SELECT
    MEDIAN(numeric_column),
    PERCENTILE(numeric_column, 0.99)
FROM my_table;
```

