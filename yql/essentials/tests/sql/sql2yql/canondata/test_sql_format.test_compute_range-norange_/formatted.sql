/* syntax version 1 */
/* postgres can not */
/* yt can not */
PRAGMA warning('disable', '4510');

$opt_type = Struct<x: Int32?, y: Int32?, z: String?, t: String?>;
$xy_keys = AsTuple(AsAtom('x'), AsAtom('y'));
$range_for = ($pred) -> (YQL::RangeComputeFor($opt_type, $pred, $xy_keys));
$pred1 = ($row) -> (($row.x + $row.y > 0) ?? FALSE);
$pred2 = ($row) -> (($row.x > 0 OR $row.y > 0) ?? FALSE);
$pred3 = ($row) -> (($row.x > 0 OR $row.z == 'test') ?? FALSE);

SELECT
    $range_for($pred1) IS NULL,
    $range_for($pred2) IS NULL,
    $range_for($pred3) IS NULL,
;
