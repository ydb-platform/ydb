/* syntax version 1 */
/* postgres can not */
/* yt can not */
PRAGMA warning('disable', '4510');

$opt_type = Struct<x: UInt32?, y: Int32?>;
$xy_keys = AsTuple(AsAtom('x'), AsAtom('y'));
$range_for = ($pred) -> (YQL::RangeComputeFor($opt_type, $pred, $xy_keys));
$pred1 = ($row) -> (($row.x == 3u AND ($row.y > 300 OR $row.y == 100)) ?? FALSE);
$pred2 = ($row) -> (($row.y > 300 AND $row.x == 3u OR $row.x == 3u AND $row.y == 100) ?? FALSE);

SELECT
    $range_for($pred1),
    $range_for($pred2),
;
