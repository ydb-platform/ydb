/* syntax version 1 */
/* postgres can not */
$f = AGGREGATION_FACTORY("sum");
$g = AggregateTransformOutput($f, ($x)->(cast($x as String)));
$h = AggregateTransformOutput($f, ($x)->($x * 2));
select ListAggregate([1,2,3], $f);
select ListAggregate([1,2,3], $g);
select ListAggregate([1,2,3], $h);