# Агрегатные функции

{% note info %}

Агрегатные функции не учитывают `NULL` в своих аргументах, за исключением функции `COUNT`, если в качестве аргумента указана `*`, а также функций `BOOL_AND` / `BOOL_OR` / `BOOL_XOR`, в которых `NULL` учитывается без пропусков.

{% endnote %}

## COUNT {#count}

#### Сигнатура

```yql
COUNT(*)->Uint64
COUNT(T)->Uint64
COUNT(T?)->Uint64
```

Подсчет количества строк в строковой или колоночной таблице (если в качестве аргумента указана `*` или константа) или непустых значений в столбце таблицы (если в качестве аргумента указано имя столбца).

Как и другие агрегатные функции, может использоваться в сочетании с [GROUP BY](../syntax/group_by.md) для получения статистики по частям таблицы, соответствующим значениям в столбцах, по которым идет группировка. А модификатор [DISTINCT](../syntax/group_by.md#distinct) позволяет посчитать число уникальных значений.

#### Примеры

```yql
SELECT COUNT(*) FROM my_table;
```

```yql
SELECT key, COUNT(value) FROM my_table GROUP BY key;
```

```yql
SELECT COUNT(DISTINCT value) FROM my_table;
```

## MIN и MAX {#min-max}

#### Сигнатура

```yql
MIN(T?)->T?
MIN(T)->T?
MAX(T?)->T?
MAX(T)->T?
```

Минимальное или максимальное значение.

В качестве аргумента допустимо произвольное вычислимое выражение с результатом, допускающим сравнение значений.

#### Примеры

```yql
SELECT MIN(value), MAX(value) FROM my_table;
```

## SUM {#sum}

#### Сигнатура

```yql
SUM(Unsigned?)->Uint64?
SUM(Signed?)->Int64?
SUM(Interval?)->Interval?
SUM(Decimal(N, M)?)->Decimal(35, M)?
```

Сумма чисел.

В качестве аргумента допустимо произвольное вычислимое выражение с числовым результатом или типом `Interval`.

Целые числа автоматически расширяются до 64 бит, чтобы уменьшить риск переполнения.

```yql
SELECT SUM(value) FROM my_table;
```

## AVG {#avg}

#### Сигнатура

```yql
AVG(Double?)->Double?
AVG(Interval?)->Interval?
AVG(Decimal(N, M)?)->Decimal(N, M)?
```

Арифметическое среднее.

В качестве аргумента допустимо произвольное вычислимое выражение с числовым результатом или типом `Interval`.

Целочисленные значения и интервалы времени автоматически приводятся к Double.

#### Примеры

```yql
SELECT AVG(value) FROM my_table;
```

## COUNT_IF {#count-if}

#### Сигнатура

```yql
COUNT_IF(Bool?)->Uint64
```

Количество строк, для которых указанное в качестве аргумента выражение истинно (результат вычисления выражения — true).

Значение `NULL` приравнивается к `false` (в случае, если тип аргумента `Bool?`).

Функция *не* выполняет неявного приведения типов к булевым для строк и чисел.

#### Примеры

```yql
SELECT
  COUNT_IF(value % 2 == 1) AS odd_count
```

{% note info %}

Если нужно посчитать число уникальных значений на строках, где выполняется условие, то в отличие от остальных агрегатных функций модификатор [DISTINCT](../syntax/group_by.md#distinct) тут не поможет, так как в аргументах нет никаких значений. Для получения данного результата, стоит воспользоваться в подзапросе встроенной функцией [IF](../builtins/basic.md#if) с двумя аргументами (чтобы в else получился `NULL`), а снаружи сделать [COUNT(DISTINCT ...)](#count) по её результату.

{% endnote %}

## SUM_IF и AVG_IF {#sum-if}

#### Сигнатура

```yql
SUM_IF(Unsigned?, Bool?)->Uint64?
SUM_IF(Signed?, Bool?)->Int64?
SUM_IF(Interval?, Bool?)->Interval?

AVG_IF(Double?, Bool?)->Double?
```

Сумма или арифметическое среднее, но только для строк, удовлетворяющих условию, переданному вторым аргументом.

Таким образом, `SUM_IF(value, condition)` является чуть более короткой записью для `SUM(IF(condition, value))`, аналогично для `AVG`. Расширение типа данных аргумента работает так же аналогично одноименным функциям без суффикса.

#### Примеры

```yql
SELECT
    SUM_IF(value, value % 2 == 1) AS odd_sum,
    AVG_IF(value, value % 2 == 1) AS odd_avg,
FROM my_table;
```

При использовании [фабрики агрегационной функции](basic.md#aggregationfactory) в качестве первого аргумента [AGGREGATE_BY](#aggregate-by) передается `Tuple` из значения и предиката.

```yql
$sum_if_factory = AggregationFactory("SUM_IF");
$avg_if_factory = AggregationFactory("AVG_IF");

SELECT
    AGGREGATE_BY(AsTuple(value, value % 2 == 1), $sum_if_factory) AS odd_sum,
    AGGREGATE_BY(AsTuple(value, value % 2 == 1), $avg_if_factory) AS odd_avg
FROM my_table;
```

## SOME {#some}

#### Сигнатура

```yql
SOME(T?)->T?
SOME(T)->T?
```

Получить значение указанного в качестве аргумента выражения для одной из строк таблицы. Не дает никаких гарантий о том, какая именно строка будет использована.

Из-за отсутствия гарантий `SOME` вычислительно дешевле, чем часто использующиеся в подобных ситуациях [MIN и MAX](#min-max).

#### Примеры

```yql
SELECT
  SOME(value)
FROM my_table;
```

{% note alert %}

При вызове агрегатной функции `SOME` несколько раз **не** гарантируется, что все значения результатов будут взяты с одной строки исходной таблицы. Для получения данной гарантии, нужно запаковать значения в какой-либо из контейнеров и передавать в `SOME` уже его. Например, для структуры это можно сделать с помощью [AsStruct](basic.md#asstruct)

{% endnote %}

## CountDistinctEstimate, HyperLogLog и HLL {#countdistinctestimate}

#### Сигнатура

```yql
CountDistinctEstimate(T)->Uint64?
HyperLogLog(T)->Uint64?
HLL(T)->Uint64?
```

Примерная оценка числа уникальных значений по алгоритму [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog). Логически делает то же самое, что и [COUNT(DISTINCT ...)](#count), но работает значительно быстрее ценой некоторой погрешности.

Аргументы:

1. Значение для оценки;
2. Точность (от 4 до 18 включительно, по умолчанию 14).

Выбор точности позволяет разменивать дополнительное потребление вычислительных ресурсов и оперативной памяти на уменьшение погрешности.

На данный момент все три функции являются алиасами, но в будущем `CountDistinctEstimate` может начать использовать другой алгоритм.

#### Примеры

```yql
SELECT
  CountDistinctEstimate(my_column)
FROM my_table;
```

```yql
SELECT
  HyperLogLog(my_column, 4)
FROM my_table;
```



## AGGREGATE_LIST {#agg-list}

#### Сигнатура

```yql
AGGREGATE_LIST(T? [, limit:Uint64])->List<T>
AGGREGATE_LIST(T [, limit:Uint64])->List<T>
AGGREGATE_LIST_DISTINCT(T? [, limit:Uint64])->List<T>
AGGREGATE_LIST_DISTINCT(T [, limit:Uint64])->List<T>
```

Получить все значения столбца в виде списка. В сочетании с `DISTINCT` возвращает только уникальные значения. Опциональный второй параметр задает максимальное количество получаемых значений. Значение 0 равносильно отсутствию лимита.

Если заранее известно, что уникальных значений не много, то лучше воспользоваться агрегатной функцией `AGGREGATE_LIST_DISTINCT`, которая строит тот же результат в памяти (которой при большом числе уникальных значений может не хватить).

Порядок элементов в результирующем списке зависит от реализации и снаружи не задается. Чтобы получить упорядоченный список, необходимо отсортировать результат, например с помощью [ListSort](list.md#listsort).

Чтобы получить список нескольких значений с одной строки, важно *НЕ* использовать функцию `AGGREGATE_LIST` несколько раз, а сложить все нужные значения в контейнер, например через [AsList](basic.md#aslist) или [AsTuple](basic.md#astuple) и передать этот контейнер в один вызов `AGGREGATE_LIST`.

Например, можно использовать в сочетании с `DISTINCT` и функцией [String::JoinFromList](../udf/list/string.md) (аналог `','.join(list)` из Python) для распечатки в строку всех значений, которые встретились в столбце после применения [GROUP BY](../syntax/group_by.md).

#### Примеры

```yql
SELECT
   AGGREGATE_LIST( region ),
   AGGREGATE_LIST( region, 5 ),
   AGGREGATE_LIST( DISTINCT region ),
   AGGREGATE_LIST_DISTINCT( region ),
   AGGREGATE_LIST_DISTINCT( region, 5 )
FROM users
```

```yql
-- Аналог GROUP_CONCAT из MySQL
SELECT
    String::JoinFromList(CAST(AGGREGATE_LIST(region, 2) AS List<String>), ",")
FROM users
```

Существует также короткая форма записи этих функций - `AGG_LIST` и `AGG_LIST_DISTINCT`.

{% note alert %}

Выполняется **НЕ** ленивым образом, поэтому при использовании нужно быть уверенным, что список получится разумных размеров, примерно в пределах тысячи элементов. Чтобы подстраховаться, можно воспользоваться вторым опциональным числовым аргументом, который включает ограничение на число элементов в списке.

{% endnote %}


## MAX_BY и MIN_BY {#max-min-by}

#### Сигнатура

```yql
MAX_BY(T1?, T2)->T1?
MAX_BY(T1, T2)->T1?
MAX_BY(T1, T2, limit:Uint64)->List<T1>?

MIN_BY(T1?, T2)->T1?
MIN_BY(T1, T2)->T1?
MIN_BY(T1, T2, limit:Uint64)->List<T1>?
```

Вернуть значение первого аргумента для строки таблицы, в которой второй аргумент оказался минимальным/максимальным.

Опционально можно указать третий аргумент N, который влияет на поведение в случае, если в таблице есть несколько строк с одинаковым минимальным или максимальным значением:

* Если N не указано — будет возвращено значение одной из строк, а остальные отбрасываются.
* Если N указано — будет возвращен список со всеми значениями, но не более N, все значения после достижения указанного числа отбрасываются.

При выборе значения N рекомендуется не превышать порядка сотен или тысяч, чтобы не возникало проблем с лимтами памяти.

Если для задачи обязательно нужны все значения, и их количество может измеряться десятками тысяч и больше, то вместо данных агрегационных функций следует использовать `JOIN` исходной таблицы с подзапросом, где по ней же сделан `GROUP BY + MIN/MAX` на интересующих вас колонках.

{% note warning %}

Если второй аргумент всегда `NULL`, то результатом агрегации будет `NULL`.

{% endnote %}

При использовании [фабрики агрегационной функции](basic.md#aggregationfactory) в качестве первого аргумента [AGGREGATE_BY](#aggregate-by) передается `Tuple` из значения и ключа.

#### Примеры

```yql
SELECT
  MIN_BY(value, LENGTH(value)),
  MAX_BY(value, key, 100)
FROM my_table;
```

```yql
$min_by_factory = AggregationFactory("MIN_BY");
$max_by_factory = AggregationFactory("MAX_BY", 100);

SELECT
    AGGREGATE_BY(AsTuple(value, LENGTH(value)), $min_by_factory),
    AGGREGATE_BY(AsTuple(value, key), $max_by_factory)
FROM my_table;
```


## TOP и BOTTOM {#top-bottom}

#### Сигнатура

```yql
TOP(T?, limit:Uint32)->List<T>
TOP(T, limit:Uint32)->List<T>
BOTTOM(T?, limit:Uint32)->List<T>
BOTTOM(T, limit:Uint32)->List<T>
```

Вернуть список максимальных/минимальных значений выражения. Первый аргумент - выражение, второй - ограничение на количество элементов.

#### Примеры

```yql
SELECT
    TOP(key, 3),
    BOTTOM(value, 3)
FROM my_table;
```

```yql
$top_factory = AggregationFactory("TOP", 3);
$bottom_factory = AggregationFactory("BOTTOM", 3);

SELECT
    AGGREGATE_BY(key, $top_factory),
    AGGREGATE_BY(value, $bottom_factory)
FROM my_table;
```

## TOP_BY и BOTTOM_BY {#top-bottom-by}

#### Сигнатура

```yql
TOP_BY(T1, T2, limit:Uint32)->List<T1>
BOTTOM_BY(T1, T2, limit:Uint32)->List<T1>
```

Вернуть список значений первого аргумента для строк с максимальными/минимальными значениями второго аргумента. Третий аргумент - ограничение на количество элементов в списке.

При использовании [фабрики агрегационной функции](basic.md#aggregationfactory) в качестве первого аргумента [AGGREGATE_BY](#aggregate-by) передается `Tuple` из значения и ключа. Ограничение на количество элементов в этом случае передаётся вторым аргументом при создании фабрики.

#### Примеры

```yql
SELECT
    TOP_BY(value, LENGTH(value), 3),
    BOTTOM_BY(value, key, 3)
FROM my_table;
```

```yql
$top_by_factory = AggregationFactory("TOP_BY", 3);
$bottom_by_factory = AggregationFactory("BOTTOM_BY", 3);

SELECT
    AGGREGATE_BY(AsTuple(value, LENGTH(value)), $top_by_factory),
    AGGREGATE_BY(AsTuple(value, key), $bottom_by_factory)
FROM my_table;
```


## TOPFREQ и MODE {#topfreq-mode}

#### Сигнатура

```yql
TOPFREQ(T [, num:Uint32 [, bufSize:Uint32]])->List<Struct<Frequency:Uint64, Value:T>>
MODE(T [, num:Uint32 [, bufSize:Uint32]])->List<Struct<Frequency:Uint64, Value:T>>
```

Получение **приближенного** списка самых часто встречающихся значений колонки с оценкой их числа. Возвращают список структур с двумя полями:

* `Value`— найденное часто встречающееся значение;
* `Frequency` — оценка числа упоминаний в таблице.

Обязательный аргумент: само значение.

Опциональные аргументы:

1. Для `TOPFREQ` — желаемое число элементов в результате. `MODE` является алиасом к `TOPFREQ` с 1 в этом аргументе. У `TOPFREQ` по умолчанию тоже 1.
2. Число элементов в используемом буфере, что позволяет разменивать потребление памяти на точность. По умолчанию 100.

#### Примеры

```yql
SELECT
    MODE(my_column),
    TOPFREQ(my_column, 5, 1000)
FROM my_table;
```


## STDDEV и VARIANCE {#stddev-variance}

#### Сигнатура

```yql
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

#### Примеры

```yql
SELECT
  STDDEV(numeric_column),
  VARIANCE(numeric_column)
FROM my_table;
```


## CORRELATION и COVARIANCE {#correlation-covariance}

#### Сигнатура

```yql
CORRELATION(Double?, Double?)->Double?
COVARIANCE(Double?, Double?)->Double?
COVARIANCE_SAMPLE(Double?, Double?)->Double?
COVARIANCE_POPULATION(Double?, Double?)->Double?
```

Корреляция и ковариация двух колонок.

Также доступны сокращенные версии `CORR` или `COVAR`, а для ковариации - версии с суффиксом `SAMPLE` / `POPULATION` по аналогии с описанной выше [VARIANCE](#stddev-variance).

В отличие от большинства других агрегатных функций не пропускают `NULL`, а считают его за 0.

При использовании [фабрики агрегационной функции](basic.md#aggregationfactory) в качестве первого аргумента [AGGREGATE_BY](#aggregate-by) передается `Tuple` из двух значений.

#### Примеры

```yql
SELECT
  CORRELATION(numeric_column, another_numeric_column),
  COVARIANCE(numeric_column, another_numeric_column)
FROM my_table;
```

```yql
$corr_factory = AggregationFactory("CORRELATION");

SELECT
    AGGREGATE_BY(AsTuple(numeric_column, another_numeric_column), $corr_factory)
FROM my_table;
```

## PERCENTILE и MEDIAN {#percentile-median}

#### Сигнатура

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

#### Примеры

```yql
SELECT
    MEDIAN(numeric_column),
    PERCENTILE(numeric_column, 0.99),
    PERCENTILE(CAST(string_column as Double), (0.01, 0.5, 0.99)),                   -- подсчет сразу трех перцентилей
    PERCENTILE(numeric_column, AsStruct(0.01 as p01, 0.5 as median, 0.99 as p99)), -- используя структуру, значениям перцентиля можно дать удобные имена
    PERCENTILE(numeric_column, ListFromRange(0.00, 1.05, 0.05)),                   -- подсчет множества перцентилей (от 0.0 до 1.0 включительно с шагом 0.05)
FROM my_table;
```



## HISTOGRAM {#histogram}

#### Сигнатура

```yql
HISTOGRAM(Double?)->HistogramStruct?
HISTOGRAM(Double?, weight:Double)->HistogramStruct?
HISTOGRAM(Double?, intervals:Uint32)->HistogramStruct?
HISTOGRAM(Double?, weight:Double, intervals:Uint32)->HistogramStruct?
```

В описании сигнатур под HistogramStruct подразумевается результат работы агрегатной функции, который является структурой определенного вида.

Построение примерной гистограммы по числовому выражению с автоматическим выбором корзин.

[Вспомогательные функции](../udf/list/histogram.md)

### Базовые настройки

Ограничение на число корзин можно задать с помощью опционального аргумента, значение по умолчанию — 100. Следует иметь в виду, что дополнительная точность стоит дополнительных вычислительных ресурсов и может негативно сказываться на времени выполнения запроса, а в экстремальных случаях — и на его успешности.

### Поддержка весов

Имеется возможность указать «вес» для каждого значения, участвующего в построении гистограммы. Для этого вторым аргументом в агрегатную функцию нужно передать выражение для вычисления веса. По умолчанию всегда используется вес `1.0`. Если используются нестандартные веса, ограничение на число корзин можно задать третьим аргументом.

В случае, если передано два аргумента, смысл второго аргумента определяется по его типу (целочисленный литерал — ограничение на число корзин, в противном случае — вес).

### Алгоритмы

* [Оригинальный whitepaper](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf);

Доступны разные модификации алгоритма:

```yql
AdaptiveDistanceHistogram
AdaptiveWeightHistogram
AdaptiveWardHistogram
BlockWeightHistogram
BlockWardHistogram
```

По умолчанию `HISTOGRAM` является синонимом к `AdaptiveWardHistogram`. Обе функции эквивалентны и взаимозаменимы во всех контекстах.

Алгоритмы Distance, Weight и Ward отличаются формулами объединения двух точек в одну:

```c++
TWeightedValue CalcDistanceQuality(const TWeightedValue& left, const TWeightedValue& right) {
    return TWeightedValue(right.first - left.first, left.first);
}

TWeightedValue CalcWeightQuality(const TWeightedValue& left, const TWeightedValue& right) {
    return TWeightedValue(right.second + left.second, left.first);
}

TWeightedValue CalcWardQuality(const TWeightedValue& left, const TWeightedValue& right) {
    const double N1 = left.second;
    const double N2 = right.second;
    const double mu1 = left.first;
    const double mu2 = right.first;
    return TWeightedValue(N1 * N2 / (N1 + N2) * (mu1 - mu2) * (mu1 - mu2), left.first);
}
```

Чем отличается Adaptive и Block:

{% block info %}

Contrary to adaptive histogram, block histogram doesn't rebuild bins after the addition of each point. Instead, it accumulates points and in case the amount of points overflows specified limits, it shrinks all the points at once to produce histogram. Indeed, there exist two limits and two shrinkage operations:

1. FastGreedyShrink is fast but coarse. It is used to shrink from upper limit to intermediate limit (override FastGreedyShrink to set specific behaviour).
2. SlowShrink is slow, but produces finer histogram. It shrinks from the intermediate limit to the actual number of bins in a manner similar to that in adaptive histogram (set CalcQuality in constuctor)

While FastGreedyShrink is used most of the time, SlowShrink is mostly used for histogram finalization

{% endblock %}

### Если нужна точная гистограмма

1. Можно воспользоваться описанными ниже агрегатными функциями с фиксированными сетками корзин: [LinearHistogram](#linearhistogram) или [LogarithmicHistogram](#linearhistogram).
2. Можно самостоятельно вычислить номер корзины для каждой строки и сделать по нему [GROUP BY](../syntax/group_by.md).

При использовании [фабрики агрегационной функции](basic.md#aggregationfactory) в качестве первого аргумента [AGGREGATE_BY](#aggregate-by) передается `Tuple` из значения и веса.

#### Примеры

```yql
SELECT
    HISTOGRAM(numeric_column)
FROM my_table;
```

```yql
SELECT
    Histogram::Print(
        HISTOGRAM(numeric_column, 10),
        50
    )
FROM my_table;
```

```yql
$hist_factory = AggregationFactory("HISTOGRAM");

SELECT
    AGGREGATE_BY(AsTuple(numeric_column, 1.0), $hist_factory)
FROM my_table;
```

## LinearHistogram, LogarithmicHistogram и LogHistogram {#linearhistogram}

Построение гистограммы по явно указанной фиксированной шкале корзин.

#### Сигнатура

```yql
LinearHistogram(Double?)->HistogramStruct?
LinearHistogram(Double? [, binSize:Double [, min:Double [, max:Double]]])->HistogramStruct?

LogarithmicHistogram(Double?)->HistogramStruct?
LogarithmicHistogram(Double? [, logBase:Double [, min:Double [, max:Double]]])->HistogramStruct?
LogHistogram(Double?)->HistogramStruct?
LogHistogram(Double? [, logBase:Double [, min:Double [, max:Double]]])->HistogramStruct?
```

Аргументы:

1. Выражение, по значению которого строится гистограмма. Все последующие — опциональны.
2. Расстояние между корзинами для `LinearHistogram` или основание логарифма для `LogarithmicHistogram` / `LogHistogram` (это алиасы). В обоих случаях значение по умолчанию  — 10.
3. Минимальное значение. По умолчанию минус бесконечность.
4. Максимальное значение. По умолчанию плюс бесконечность.

Формат результата полностью аналогичен [адаптивным гистограммам](#histogram), что позволяет использовать тот же [набор вспомогательных функций](../udf/list/histogram.md).

Если разброс входных значений неконтролируемо велик, рекомендуется указывать минимальное и максимальное значение для предотвращения потенциальных падений из-за высокого потребления памяти.

#### Примеры

```yql
SELECT
    LogarithmicHistogram(numeric_column, 2)
FROM my_table;
```

## CDF (cumulative distribution function) {#histogramcdf}

К каждому виду функции Histogram можно приписать суффикс CDF для построения кумулятивной функции распределения. Конструкции

```yql
SELECT
    Histogram::ToCumulativeDistributionFunction(Histogram::Normalize(<вид_функции>Histogram(numeric_column)))
FROM my_table;
```

и

```yql
SELECT
    <вид_функции>HistogramCDF(numeric_column)
FROM my_table;
```

полностью эквивалентны.

## BOOL_AND, BOOL_OR и BOOL_XOR {#bool-and-or-xor}

#### Сигнатура

```yql
BOOL_AND(Bool?)->Bool?
BOOL_OR(Bool?)->Bool?
BOOL_XOR(Bool?)->Bool?
```

Применение соответствующей логической операции (`AND`/`OR`/`XOR`) ко всем значениям булевой колонки или выражения.

В отличие от большинства агрегатных функций, эти функции **не пропускают** `NULL` значение при агрегации и действуют по правилу:

- `true AND null == null`
- `false OR null == null`

Для `BOOL_AND`:

- Для любого количества значений `true` и хотя бы одного `NULL` значения, результатом будет `NULL`.
- В случае хотя бы одного `false` значения, результатом будет `false`, независимо от наличия `NULL`.

Для `BOOL_OR`:

- Для любого количества значений `false` и хотя бы одного `NULL` значения, результатом будет `NULL`.
- В случае хотя бы одного `true` значения, результатом будет `true`, независимо от наличия `NULL`.

Для `BOOL_XOR`:

- В случае хотя бы одного `NULL` значения, результатом будет `NULL`.

Примеры описанного поведения приведены ниже.

Для агрегации с пропуском `NULL`-ов можно использовать функции `MIN`/`MAX` или `BIT_AND`/`BIT_OR`/`BIT_XOR`.

#### Примеры

```yql
$data = [
    <|nonNull: true, nonFalse: true, nonTrue: NULL, anyVal: true|>,
    <|nonNull: false, nonFalse: NULL, nonTrue: NULL, anyVal: NULL|>,
    <|nonNull: false, nonFalse: NULL, nonTrue: false, anyVal: false|>,
];

SELECT
    BOOL_AND(nonNull) as nonNullAnd,      -- false
    BOOL_AND(nonFalse) as nonFalseAnd,    -- NULL
    BOOL_AND(nonTrue) as nonTrueAnd,      -- false
    BOOL_AND(anyVal) as anyAnd,           -- false
    BOOL_OR(nonNull) as nonNullOr,        -- true
    BOOL_OR(nonFalse) as nonFalseOr,      -- true
    BOOL_OR(nonTrue) as nonTrueOr,        -- NULL
    BOOL_OR(anyVal) as anyOr,             -- true
    BOOL_XOR(nonNull) as nonNullXor,      -- true
    BOOL_XOR(nonFalse) as nonFalseXor,    -- NULL
    BOOL_XOR(nonTrue) as nonTrueXor,      -- NULL
    BOOL_XOR(anyVal) as anyXor,           -- NULL
FROM AS_TABLE($data);
```

## BIT_AND, BIT_OR и BIT_XOR {#bit-and-or-xor}

Применение соответствующей битовой операции ко всем значениям числовой колонки или выражения.

#### Примеры

```yql
SELECT
    BIT_XOR(unsigned_numeric_value)
FROM my_table;
```

## SessionStart {#session-start}

Без аргументов. Допускается только при наличии [SessionWindow](../syntax/group_by.md#session-window) в
[GROUP BY](../syntax/group_by.md) / [PARTITION BY](../syntax/window.md#partition).
Возвращает значение ключевой колонки `SessionWindow`. В случае `SessionWindow` с двумя аргументами – минимальное значение первого аргумента внутри группы/раздела.
В случае раширенного варианта `SessionWindoow` – значение второго элемента кортежа, возвращаемого `<calculate_lambda>`, при котором первый элемент кортежа равен `True`.


## AGGREGATE_BY и MULTI_AGGREGATE_BY {#aggregate-by}

Применение [фабрики агрегационной функции](basic.md#aggregationfactory) ко всем значениям колонки или выражения. Функция `MULTI_AGGREGATE_BY` требует, чтобы в значении колонки или выражения была структура, кортеж или список, и применяет фабрику поэлементно, размещая результат в контейнере той же формы. Если в разных значениях колонки или выражения содержатся списки разной длины, результирующий список будет иметь наименьшую из длин этих списков.

1. Колонка, `DISTINCT` колонка или выражение;
2. Фабрика.

#### Примеры

```yql
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

