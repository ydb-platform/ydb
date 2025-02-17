/* syntax version 1 */
/* postgres can not */
/* yt can not */

pragma warning("disable", "4510");

$type = Struct<x:Int32, y:Int32, z:Int32>;
$keys = AsTuple(AsAtom("x"), AsAtom("y"), AsAtom("z"));

$range_for = ($pred) -> (YQL::RangeComputeFor($type, $pred, $keys));

$pred1 = ($row) -> (($row.x, $row.y, $row.z) >= (11, 22, 33));
$pred2 = ($row) -> (($row.x, $row.y, $row.z) >  (11, 22, 33));

$pred3 = ($row) -> (($row.x, $row.y, $row.z) <  (11, 22, 33));
$pred4 = ($row) -> (($row.x, $row.y, $row.z) <= (11, 22, 33));

$pred5 = ($row) -> (($row.x, $row.y, $row.z) >  (111, 222, 333) and ($row.x, $row.y, $row.z) <= (111, 333, 444));
$pred6 = ($row) -> (($row.x, $row.y, $row.z) >= (111, 222, 333) and ($row.x, $row.y, $row.z)  < (111, 222, 333));

select
   $range_for($pred1),
   $range_for($pred2),
   $range_for($pred3),
   $range_for($pred4),
   $range_for($pred5),
   $range_for($pred6),
;

$pred1 = ($row) -> (($row.x, $row.y) >= (11, 22));
$pred2 = ($row) -> (($row.x, $row.y) >  (11, 22));

$pred3 = ($row) -> (($row.x, $row.y) <  (11, 22));
$pred4 = ($row) -> (($row.x, $row.y) <= (11, 22));

$pred5 = ($row) -> (($row.x, $row.y) >  (111, 222) and ($row.x, $row.y) <= (111, 333));
$pred6 = ($row) -> (($row.x, $row.y) >= (111, 222) and ($row.x, $row.y)  < (111, 222));

select
   $range_for($pred1),
   $range_for($pred2),
   $range_for($pred3),
   $range_for($pred4),
   $range_for($pred5),
   $range_for($pred6),
;
