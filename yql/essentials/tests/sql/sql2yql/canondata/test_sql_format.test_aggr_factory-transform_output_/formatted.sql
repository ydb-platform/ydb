/* syntax version 1 */
/* postgres can not */
$f = AGGREGATION_FACTORY('sum');
$g = AggregateTransformOutput($f, ($x) -> (CAST($x AS String)));
$h = AggregateTransformOutput($f, ($x) -> ($x * 2));

SELECT
    ListAggregate([1, 2, 3], $f)
;

SELECT
    ListAggregate([1, 2, 3], $g)
;

SELECT
    ListAggregate([1, 2, 3], $h)
;
