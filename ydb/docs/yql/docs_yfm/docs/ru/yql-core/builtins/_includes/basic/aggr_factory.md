## AggregationFactory {#aggregationfactory}

Создать фабрику для [агрегационных функций](../../aggregation.md) для того чтобы разделить процесс описания того, как агрегировать данные, и то, к каким данным это применять.

Аргументы:

1. Строка в кавычках, являющаяся именем агрегационной функции, например ["MIN"](../../aggregation.md#min).
2. Опциональные параметры агрегационной функции, которые не зависят от данных. Например, значение percentile в [PERCENTILE](../../aggregation.md#percentile).

Полученную фабрику можно использовать как второй параметр функции [AGGREGATE_BY](../../aggregation.md#aggregateby).
Если агрегационная функция работает на двух колонках вместо одной, как например, [MIN_BY](../../aggregation.md#minby), то в [AGGREGATE_BY](../../aggregation.md#aggregateby) первым аргументом передается `Tuple` из двух значений. Подробнее это указано при описании такой агрегационной функции.

**Примеры:**
``` yql
$factory = AggregationFactory("MIN");
SELECT
    AGGREGATE_BY(value, $factory) AS min_value -- применить MIN агрегацию к колонке value
FROM my_table;
```

## AggregateTransform... {#aggregatetransform}

`AggregateTransformInput()` преобразует фабрику для [агрегационных функций](../../aggregation.md), например, полученную через функцию [AggregationFactory](#aggregationfactory) в другую фабрику, в которой перед началом выполнения агрегации производится указанное преобразование входных элементов.

Аргументы:

1. Фабрика для агрегационных функций;
2. Лямбда функция с одним аргументом, преобразующая входной элемент.

**Примеры:**
``` yql
$f = AggregationFactory("sum");
$g = AggregateTransformInput($f, ($x) -> (cast($x as Int32)));
$h = AggregateTransformInput($f, ($x) -> ($x * 2));
select ListAggregate([1,2,3], $f); -- 6
select ListAggregate(["1","2","3"], $g); -- 6
select ListAggregate([1,2,3], $h); -- 12
```

`AggregateTransformOutput()` преобразует фабрику для [агрегационных функций](../../aggregation.md), например, полученную через функцию [AggregationFactory](#aggregationfactory) в другую фабрику, в которой после окончания выполнения агрегации производится указанное преобразование результата.

Аргументы:

1. Фабрика для агрегационных функций;
2. Лямбда функция с одним аргументом, преобразующая результат.

**Примеры:**
``` yql
$f = AggregationFactory("sum");
$g = AggregateTransformOutput($f, ($x) -> ($x * 2));
select ListAggregate([1,2,3], $f); -- 6
select ListAggregate([1,2,3], $g); -- 12
```

## AggregateFlatten {#aggregateflatten}

Адаптирует фабрику для [агрегационных функций](../../aggregation.md), например, полученную через функцию [AggregationFactory](#aggregationfactory) так, чтобы выполнять агрегацию над входными элементами - списками. Эта операция похожа на [FLATTEN LIST BY](../../../syntax/flatten.md) - производится агрегация каждого элемента списка.

Аргументы:

1. Фабрика для агрегационных функций.

**Примеры:**
``` yql
$i = AggregationFactory("AGGREGATE_LIST_DISTINCT");
$j = AggregateFlatten($i);
select AggregateBy(x, $j) from (
   select [1,2] as x
   union all
   select [2,3] as x
); -- [1, 2, 3]

```