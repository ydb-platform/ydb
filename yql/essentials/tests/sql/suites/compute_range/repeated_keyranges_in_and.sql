/* syntax version 1 */
/* postgres can not */
/* yt can not */

pragma warning("disable", "4510");
pragma warning("disable", "1108");

select YQL::RangeComputeFor(
  Struct<x:Int32, y:UInt32, z:Uint64>,
  ($row) -> ($row.x == 1 and ($row.y = 2 and $row.z > 0 and $row.z < 10 or
                              $row.y = 2 and $row.z > 8 and $row.z < 20)),
  AsTuple(AsAtom("x"), AsAtom("y"), AsAtom("z"))
);
