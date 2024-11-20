/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma warning("disable", "4510");
pragma warning("disable", "1108");

-- single over limit
select YQL::RangeComputeFor(
  Struct<x:String, y:String>,
  ($row) -> ($row.x IN CAST(ListFromRange(0, 10001) AS List<String>)),
  AsTuple(AsAtom("x"))
);

