/* syntax version 1 */
/* postgres can not */
/* yt can not */

pragma warning("disable", "4510");

$opt_type = Struct<x:UInt32?, y:Int32?>;
$xy_keys = AsTuple(AsAtom("x"), AsAtom("y"));

$range_for = ($pred) -> (YQL::RangeComputeFor($opt_type, $pred, $xy_keys));

$pred1 = ($row) -> (($row.x == 3u and ($row.y > 300 or $row.y == 100)) ?? false);
$pred2 = ($row) -> (($row.y > 300 and $row.x == 3u or $row.x == 3u and $row.y == 100) ?? false);


select
   $range_for($pred1),
   $range_for($pred2),
;
