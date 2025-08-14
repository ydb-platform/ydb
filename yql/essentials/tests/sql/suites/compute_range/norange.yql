/* syntax version 1 */
/* postgres can not */
/* yt can not */

pragma warning("disable", "4510");

$opt_type = Struct<x:Int32?, y:Int32?, z:String?, t:String?>;
$xy_keys = AsTuple(AsAtom("x"), AsAtom("y"));

$range_for = ($pred) -> (YQL::RangeComputeFor($opt_type, $pred, $xy_keys));

$pred1 = ($row) -> (($row.x + $row.y > 0) ?? false);
$pred2 = ($row) -> (($row.x > 0 or $row.y > 0) ?? false);
$pred3 = ($row) -> (($row.x > 0 or $row.z == "test") ?? false);


select
   $range_for($pred1) is null,
   $range_for($pred2) is null,
   $range_for($pred3) is null,
;
