/* syntax version 1 */
/* postgres can not */
/* yt can not */

pragma warning("disable", "4510");
pragma warning("disable", "1108");

select YQL::RangeComputeFor(
  Struct<x:String, y:String, z:String>,
  ($row) -> (
     ($row.x, $row.y, $row.z) > ("a", "b", "c") and
     ($row.x, $row.y, $row.z) < ("d", "e", "f") and
     $row.z IN AsList("t", "u", "v") and
     $row.y IN AsList("x", "y", "z") and
     (len($row.z) == 1 OR len($row.z || 'x') == 2)),
  AsTuple(AsAtom("x"), AsAtom("y"), AsAtom("z"))
);
