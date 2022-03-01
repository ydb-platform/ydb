## AggregationFactory {#aggregationfactory}

Create a factory for [aggregation functions](../../aggregation.md) to separately describe the methods of aggregation and data types subject to aggregation.

Arguments:

1. A string in double quotes with the name of an aggregate function, for example ["MIN"](../../aggregation.md#min).
2. Optional parameters of the aggregate function that are data-independent. For example, the percentile value in [PERCENTILE](../../aggregation.md#percentile).

The resulting factory can be used as the second parameter of the function [AGGREGATE_BY](../../aggregation.md#aggregateby).
If the aggregate function is applied to two columns instead of one, as, for example, [MIN_BY](../../aggregation.md#minby), then in [AGGREGATE_BY](../../aggregation.md#aggregateby), the first argument passes a `Tuple` of two values. See more details in the description of the applicable aggregate function.

**Examples:**

```yql
$factory = AggregationFactory("MIN");
SELECT
    AGGREGATE_BY (value, $factory) AS min_value -- apply the MIN aggregation to the "value" column
FROM my_table;
```

## AggregateTransform... {#aggregatetransform}

`AggregateTransformInput()` converts an [aggregation factory](../../aggregation.md), for example, obtained using the [AggregationFactory](#aggregationfactory) function, to other factory, in which the specified transformation of input items is performed before starting aggregation.

Arguments:

1. Aggregation factory.
2. A lambda function with one argument that converts an input item.

**Examples:**

```yql
$f = AggregationFactory("sum");
$g = AggregateTransformInput($f, ($x) -> (cast($x as Int32)));
$h = AggregateTransformInput($f, ($x) -> ($x * 2));
select ListAggregate([1,2,3], $f); -- 6
select ListAggregate(["1","2","3"], $g); -- 6
select ListAggregate([1,2,3], $h); -- 12
```

`AggregateTransformOutput()` converts an [aggregation factory](../../aggregation.md), for example, obtained using the [AggregationFactory](#aggregationfactory) function, to other factory, in which the specified transformation of the result is performed after ending aggregation.

Arguments:

1. Aggregation factory.
2. A lambda function with one argument that converts the result.

**Examples:**

```yql
$f = AggregationFactory("sum");
$g = AggregateTransformOutput($f, ($x) -> ($x * 2));
select ListAggregate([1,2,3], $f); -- 6
select ListAggregate([1,2,3], $g); -- 12
```

## AggregateFlatten {#aggregateflatten}

Adapts a factory for [aggregation functions](../../aggregation.md), for example, obtained using the [AggregationFactory](#aggregationfactory) function in a way that allows aggregation of list input items. This operation is similar to [FLATTEN LIST BY](../../../syntax/flatten.md): Each list item is aggregated.

Arguments:

1. Aggregation factory.

**Examples:**

```yql
$i = AggregationFactory("AGGREGATE_LIST_DISTINCT");
$j = AggregateFlatten($i);
select AggregateBy(x, $j) from (
   select [1,2] as x
   union all
   select [2,3] as x
); -- [1, 2, 3]
```

