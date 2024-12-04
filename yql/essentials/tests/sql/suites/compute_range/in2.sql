/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma warning("disable", "4510");
pragma warning("disable", "1108");

-- tuple
select YQL::RangeComputeFor(
  Struct<x:UInt32, y:Uint32, z:Uint32>,
  ($row) -> (($row.y, $row.x, $row.z) IN ((1,2,3), (100,200,300))),
  AsTuple(AsAtom("x"), AsAtom("y"), AsAtom("z"))
);

-- tuple with single element
select YQL::RangeComputeFor(
  Struct<x:UInt32>,
  ($row) -> (($row.x,) IN ((1,), (100,))),
  AsTuple(AsAtom("x"))
);

-- key prefix tuples
select YQL::RangeComputeFor(
  Struct<x:UInt32, y:Uint32, z:Uint32>,
  ($row) -> (($row.y, $row.x) IN ((1,2), (2,2))),
  AsTuple(AsAtom("x"), AsAtom("y"), AsAtom("z"))
);

