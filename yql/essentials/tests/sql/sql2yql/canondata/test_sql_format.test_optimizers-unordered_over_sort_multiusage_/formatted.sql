PRAGMA warning('disable', '4510');
PRAGMA config.flags('OptimizerFlags', 'UnorderedOverSortImproved');

$l = ListSort(ListFromRange(1, 5), ($x) -> (-$x));

SELECT
    ListSum(YQL::Unordered($l))
;

SELECT
    ListTake($l, 3)
;
