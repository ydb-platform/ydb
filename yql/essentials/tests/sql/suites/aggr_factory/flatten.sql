/* syntax version 1 */
/* postgres can not */
$f = AGGREGATION_FACTORY("sum");
$g = AggregateFlatten($f);
select
  ListAggregate([1,2,3], $f),
  ListAggregate(ListCreate(List<Int32>), $g),
  ListAggregate([ListCreate(Int32)], $g),
  ListAggregate([ListCreate(Int32),ListCreate(Int32)], $g),
  ListAggregate([[1,2]], $g),
  ListAggregate([[1,2],[3]], $g),
  ListAggregate([ListCreate(Int32),[3]], $g),
  ListAggregate([[1,2],ListCreate(Int32)], $g);

$i = AGGREGATION_FACTORY("AGGREGATE_LIST_DISTINCT");
$j = AggregateFlatten($i);
select AggregateBy(x, $j) from (
   select [1,2] as x 
   union all
   select [2,3] as x
);
