PRAGMA warning('disable', '4510');

$l = ListSort(ListFromRange(1, 5), ($x) -> (-$x));

SELECT
    ListSum(YQL::Unordered($l))
;

SELECT
    ListTake($l, 3)
;
