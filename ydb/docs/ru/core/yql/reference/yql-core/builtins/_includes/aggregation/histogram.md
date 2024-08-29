## HISTOGRAM {#histogram}

**Сигнатура**
```
HISTOGRAM(Double?)->HistogramStruct?
HISTOGRAM(Double?, weight:Double)->HistogramStruct?
HISTOGRAM(Double?, intervals:Uint32)->HistogramStruct?
HISTOGRAM(Double?, weight:Double, intervals:Uint32)->HistogramStruct?
```
В описании сигнатур под HistogramStruct подразумевается результат работы агрегатной функции, который является структурой определенного вида.

Построение примерной гистограммы по числовому выражению с автоматическим выбором корзин.

[Вспомогательные функции](../../../udf/list/histogram.md)

### Базовые настройки

Ограничение на число корзин можно задать с помощью опционального аргумента, значение по умолчанию — 100. Следует иметь в виду, что дополнительная точность стоит дополнительных вычислительных ресурсов и может негативно сказываться на времени выполнения запроса, а в экстремальных случаях — и на его успешности.

### Поддержка весов

Имеется возможность указать «вес» для каждого значения, участвующего в построении гистограммы. Для этого вторым аргументом в агрегатную функцию нужно передать выражение для вычисления веса. По умолчанию всегда используется вес `1.0`. Если используются нестандартные веса, ограничение на число корзин можно задать третьим аргументом.

В случае, если передано два аргумента, смысл второго аргумента определяется по его типу (целочисленный литерал — ограничение на число корзин, в противном случае — вес).

{% if tech %}
### Алгоритмы

* [Оригинальный whitepaper](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf);

Доступны разные модификации алгоритма:
``` yql
AdaptiveDistanceHistogram
AdaptiveWeightHistogram
AdaptiveWardHistogram
BlockWeightHistogram
BlockWardHistogram
```

По умолчанию `HISTOGRAM` является синонимом к `AdaptiveWardHistogram`. Обе функции эквивалентны и взаимозаменимы во всех контекстах.

Алгоритмы Distance, Weight и Ward отличаются формулами объединения двух точек в одну:

``` c++
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
<blockquote>Contrary to adaptive histogram, block histogram doesn't rebuild bins after the addition of each point. Instead, it accumulates points and in case the amount of points overflows specified limits, it shrinks all the points at once to produce histogram. Indeed, there exist two limits and two shrinkage operations:

1. FastGreedyShrink is fast but coarse. It is used to shrink from upper limit to intermediate limit (override FastGreedyShrink to set specific behaviour).
2. SlowShrink is slow, but produces finer histogram. It shrinks from the intermediate limit to the actual number of bins in a manner similar to that in adaptive histogram (set CalcQuality in 34constuctor)
While FastGreedyShrink is used most of the time, SlowShrink is mostly used for histogram finalization
</blockquote>
{% endif %}

### Если нужна точная гистограмма

1. Можно воспользоваться описанными ниже агрегатными функциями с фиксированными сетками корзин: [LinearHistogram](#linearhistogram) или [LogarithmicHistogram](#logarithmichistogram).
2. Можно самостоятельно вычислить номер корзины для каждой строки и сделать по нему [GROUP BY](../../../syntax/group_by.md).

При использовании [фабрики агрегационной функции](../../basic.md#aggregationfactory) в качестве первого аргумента [AGGREGATE_BY](#aggregateby) передается `Tuple` из значения и веса.

**Примеры**
``` yql
SELECT
    HISTOGRAM(numeric_column)
FROM my_table;
```

``` yql
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

**Сигнатура**
```
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

Формат результата полностью аналогичен [адаптивным гистограммам](#histogram), что позволяет использовать тот же [набор вспомогательных функций](../../../udf/list/histogram.md).

Если разброс входных значений неконтролируемо велик, рекомендуется указывать минимальное и максимальное значение для предотвращения потенциальных падений из-за высокого потребления памяти.

**Примеры**
``` yql
SELECT
    LogarithmicHistogram(numeric_column, 2)
FROM my_table;
```
## CDF (cumulative distribution function) {#histogramcdf}

К каждому виду функции Histogram можно приписать суффикс CDF для построения кумулятивной функции распределения. Конструкции
``` yql
SELECT
    Histogram::ToCumulativeDistributionFunction(Histogram::Normalize(<вид_функции>Histogram(numeric_column)))
FROM my_table;
```
и
``` yql
SELECT
    <вид_функции>HistogramCDF(numeric_column)
FROM my_table;
```
полностью эквивалентны.