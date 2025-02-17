/* syntax version 1 */
/* postgres can not */
$f = AGGREGATION_FACTORY('sum');
$g = AggregateTransformInput($f, ($x) -> (CAST($x AS Int32)));
$h = AggregateTransformInput($f, ($x) -> ($x * 2));

SELECT
    ListAggregate([1, 2, 3], $f)
;

SELECT
    ListAggregate(['1', '2', '3'], $g)
;

SELECT
    ListAggregate([1, 2, 3], $h)
;
