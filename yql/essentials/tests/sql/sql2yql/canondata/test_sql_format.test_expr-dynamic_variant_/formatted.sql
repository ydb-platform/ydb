$dt = Int32;
$tvt = Variant<$dt, $dt>;

SELECT
    ListMap([(10, 0us), (20, 2us)], ($x) -> (DynamicVariant($x.0, $x.1, $tvt)))
;

$dt = Int32;
$svt = Variant<x: $dt, y: $dt>;

SELECT
    ListMap([(10, 'x'), (20, 'z'), (30, NULL)], ($x) -> (DynamicVariant($x.0, CAST($x.1 AS utf8), $svt)))
;
