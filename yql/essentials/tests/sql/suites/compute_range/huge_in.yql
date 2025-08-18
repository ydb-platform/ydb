/* syntax version 1 */
/* postgres can not */
/* yt can not */

pragma warning("disable", "4510");
pragma warning("disable", "1108");

select YQL::RangeComputeFor(
  Struct<x:Int32>,
  ($row) -> ($row.x IN ListFromRange(100000000, 0, -1)),
  AsTuple(AsAtom("x"))
);


