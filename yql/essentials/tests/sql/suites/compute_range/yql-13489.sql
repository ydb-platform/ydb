/* syntax version 1 */
/* postgres can not */
/* yt can not */

pragma warning("disable", "4510");

select YQL::RangeComputeFor(
  Struct<x:Int32, y:Int32>,
  ($row) -> ($row.x > 2 or ($row.x == 2 and $row.y >= 10) or ($row.x == 0 and $row.y < 10)),
  AsTuple(AsAtom("x"))
);

select YQL::RangeComputeFor(
  Struct<x:Int32, y:Int32>,
  ($row) -> (($row.x > 10 and $row.y > 1) or $row.x < 5),
  AsTuple(AsAtom("x"), AsAtom("y"))
);

