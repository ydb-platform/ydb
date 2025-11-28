/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma warning("disable", "4510");
pragma warning("disable", "1108");

select YQL::RangeComputeFor(
  Struct<x:Uint32>,
  ($row) -> ($row.x IN ListFromRange(-1,10001)),
  AsTuple(AsAtom("x"))
);
