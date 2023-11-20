## PERCENTILE и MEDIAN {#percentile-median}

**Сигнатура**
```
PERCENTILE(T, Double)->T
PERCENTILE(T, Tuple<Double, ...>)->Tuple<T, ...>
PERCENTILE(T, Struct<name1:Double, ...>)->Struct<name1:T, ...>
PERCENTILE(T, List<Double>)->List<T>

MEDIAN(T, [ Double ])->T
MEDIAN(T, [ Tuple<Double, ...> ])->Tuple<T, ...>
MEDIAN(T, [ Struct<name1:Double, ...> ])->Struct<name1:T, ...>
MEDIAN(T, [ List<Double> ])->List<T>
```

Подсчет процентилей по амортизированной версии алгоритма [TDigest](https://github.com/tdunning/t-digest). `MEDIAN(x)` без второго аргумента — алиас для `PERCENTILE(x, 0.5)`.
`MEDIAN` с двумя аргументами полностью эквивалентен `PERCENTILE`.

В качестве первого аргумента `PERCENTILE`/`MEDIAN` принимает выражение типа `T`. В качестве типа `T` на данный момент поддерживаются типы `Interval` и `Double`
(а также типы которые допускают неявное приведение к ним - например целочисленные типы).

В качестве второго аргумента можно использовать либо один `Double` (значение перцентиля), либо сразу несколько значений перцентиля в виде `Tuple`/`Struct`/`List`.

Значения прецентиля должны лежать в диапазоне от `0.0` до `1.0` включительно.

**Примеры**
``` yql
SELECT
    MEDIAN(numeric_column),
    PERCENTILE(numeric_column, 0.99),
    PERCENTILE(CAST(string_column as Double), (0.01, 0.5, 0.99)),                   -- подсчет сразу трех перцентилей
    PERCENtILE(numeric_column, AsStruct(0.01 as p01, 0.5 as median, 0.99 as p99)), -- используя структуру, значениям перцентиля можно дать удобные имена
    PERCENTILE(numeric_column, ListFromRange(0.00, 1.05, 0.05)),                   -- подсчет множества перцентилей (от 0.0 до 1.0 включительно с шагом 0.05)
FROM my_table;
```

