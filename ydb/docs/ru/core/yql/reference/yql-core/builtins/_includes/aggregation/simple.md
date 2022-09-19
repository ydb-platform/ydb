## COUNT {#count}

**Сигнатура**
```
COUNT(*)->Uint64
COUNT(T)->Uint64
COUNT(T?)->Uint64
```

Подсчет количества строк в таблице (если в качестве аргумента указана `*` или константа) или непустых значений в столбце таблицы (если в качестве аргумента указано имя столбца).

Как и другие агрегатные функции, может использоваться в сочетании с [GROUP BY](../../../syntax/group_by.md) для получения статистики по частям таблицы, соответствующим значениям в столбцах, по которым идет группировка. {% if select_statement != "SELECT STREAM" %}А модификатор [DISTINCT](../../../syntax/group_by.md#distinct) позволяет посчитать число уникальных значений.{% endif %}

**Примеры**
``` yql
SELECT COUNT(*) FROM my_table;
```

``` yql
SELECT key, COUNT(value) FROM my_table GROUP BY key;
```
{% if select_statement != "SELECT STREAM" %}

``` yql
SELECT COUNT(DISTINCT value) FROM my_table;
```
{% endif %}

## MIN и MAX {#min-max}

**Сигнатура**
```
MIN(T?)->T?
MIN(T)->T?
MAX(T?)->T?
MAX(T)->T?
```

Минимальное или максимальное значение.

В качестве аргумента допустимо произвольное вычислимое выражение с результатом, допускающим сравнение значений.

**Примеры**
``` yql
SELECT MIN(value), MAX(value) FROM my_table;
```

## SUM {#sum}

**Сигнатура**
```
SUM(Unsigned?)->Uint64?
SUM(Signed?)->Int64?
SUM(Interval?)->Interval?
SUM(Decimal(N, M)?)->Decimal(35, M)?
```

Сумма чисел.

В качестве аргумента допустимо произвольное вычислимое выражение с числовым результатом или типом `Interval`.

Целые числа автоматически расширяются до 64 бит, чтобы уменьшить риск переполнения.

``` yql
SELECT SUM(value) FROM my_table;
```

## AVG {#avg}

**Сигнатура**
```
AVG(Double?)->Double?
AVG(Interval?)->Interval?
AVG(Decimal(N, M)?)->Decimal(N, M)?
```

Арифметическое среднее.

В качестве аргумента допустимо произвольное вычислимое выражение с числовым результатом или типом `Interval`.

Целочисленные значения и интервалы времени автоматически приводятся к Double.

**Примеры**
``` yql
SELECT AVG(value) FROM my_table;
```

## COUNT_IF {#count-if}

**Сигнатура**
```
COUNT_IF(Bool?)->Uint64?
```

Количество строк, для которых указанное в качестве аргумента выражение истинно (результат вычисления выражения — true).

Значение `NULL` приравнивается к `false` (в случае, если тип аргумента `Bool?`).

Функция *не* выполняет неявного приведения типов к булевым для строк и чисел.

**Примеры**
``` yql
SELECT
  COUNT_IF(value % 2 == 1) AS odd_count
```

{% if select_statement != "SELECT STREAM" %}
{% note info %}

Если нужно посчитать число уникальных значений на строках, где выполняется условие, то в отличие от остальных агрегатных функций модификатор [DISTINCT](../../../syntax/group_by.md#distinct) тут не поможет, так как в аргументах нет никаких значений. Для получения данного результата, стоит воспользоваться в подзапросе встроенной функцией [IF](../../../builtins/basic.md#if) с двумя аргументами (чтобы в else получился `NULL`), а снаружи сделать [COUNT(DISTINCT ...)](#count) по её результату.

{% endnote %}
{% endif %}

## SUM_IF и AVG_IF {#sum-if}

**Сигнатура**
```
SUM_IF(Unsigned?, Bool?)->Uint64?
SUM_IF(Signed?, Bool?)->Int64?
SUM_IF(Interval?, Bool?)->Interval?

AVG_IF(Double?, Bool?)->Double?
```

Сумма или арифметическое среднее, но только для строк, удовлетворяющих условию, переданному вторым аргументом.

Таким образом, `SUM_IF(value, condition)` является чуть более короткой записью для `SUM(IF(condition, value))`, аналогично для `AVG`. Расширение типа данных аргумента работает так же аналогично одноименным функциям без суффикса.

**Примеры**
``` yql
SELECT
    SUM_IF(value, value % 2 == 1) AS odd_sum,
    AVG_IF(value, value % 2 == 1) AS odd_avg,
FROM my_table;
```

При использовании [фабрики агрегационной функции](../../basic.md#aggregationfactory) в качестве первого аргумента [AGGREGATE_BY](#aggregateby) передается `Tuple` из значения и предиката.

**Примеры**

``` yql
$sum_if_factory = AggregationFactory("SUM_IF");
$avg_if_factory = AggregationFactory("AVG_IF");

SELECT
    AGGREGATE_BY(AsTuple(value, value % 2 == 1), $sum_if_factory) AS odd_sum,
    AGGREGATE_BY(AsTuple(value, value % 2 == 1), $avg_if_factory) AS odd_avg
FROM my_table;
```

## SOME {#some}

**Сигнатура**
```
SOME(T?)->T?
SOME(T)->T?
```

Получить значение указанного в качестве аргумента выражения для одной из строк таблицы. Не дает никаких гарантий о том, какая именно строка будет использована. Аналог функции [any()]{% if lang == "en" %}(https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/any/){% else %}(https://clickhouse.tech/docs/ru/sql-reference/aggregate-functions/reference/any/){% endif %} в ClickHouse.

Из-за отсутствия гарантий `SOME` вычислительно дешевле, чем часто использующиеся в подобных ситуациях [MIN](#min)/[MAX](#max).

**Примеры**
``` yql
SELECT
  SOME(value)
FROM my_table;
```

{% note alert %}

При вызове агрегатной функции `SOME` несколько раз **не** гарантируется, что все значения результатов будут взяты с одной строки исходной таблицы. Для получения данной гарантии, нужно запаковать значения в какой-либо из контейнеров и передавать в `SOME` уже его. Например, для структуры это можно сделать с помощью [AsStruct](../../../builtins/basic.md#asstruct)

{% endnote %}