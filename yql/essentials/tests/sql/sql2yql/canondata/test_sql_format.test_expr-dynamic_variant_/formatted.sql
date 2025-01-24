$dt = Int32;
$tvt = Variant<$dt, $dt>;

SELECT
    ListMap([(10, 0u), (20, 2u)], ($x) -> (DynamicVariant($x.0, $x.1, $tvt)))
;

$dt = Int32;
$svt = Variant<x: $dt, y: $dt>;

SELECT
    ListMap([(10, 'x'u), (20, 'z'u)], ($x) -> (DynamicVariant($x.0, $x.1, $svt)))
;
