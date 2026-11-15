### Сигнатура

```yql
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

В качестве первого аргумента `PERCENTILE`/`MEDIAN` принимает выражение типа `T`. В качестве типа `T` на данный момент поддерживаются типы `Interval` и `Double` (а также типы которые допускают неявное приведение к ним - например целочисленные типы).

В качестве второго аргумента можно использовать либо один `Double` (значение перцентиля), либо сразу несколько значений перцентиля в виде `Tuple`/`Struct`/`List`.

Значения прецентиля должны лежать в диапазоне от 0.0 до 1.0 включительно.

### Примеры
