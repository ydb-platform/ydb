/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma warning("disable", "4510");
pragma warning("disable", "1108");

select YQL::RangeComputeFor(
  Struct<x:Int32?>,
  ($row) -> (($row.x IN (1,2,5,null)) ?? false),
  AsTuple(AsAtom("x"))
);
