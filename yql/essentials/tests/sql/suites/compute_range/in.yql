/* syntax version 1 */
/* postgres can not */
/* yt can not */

pragma warning("disable", "4510");
pragma warning("disable", "1108");

-- basic IN
select YQL::RangeComputeFor(
  Struct<x:UInt32>,
  ($row) -> ($row.x IN (1, 2, -1)),
  AsTuple(AsAtom("x"))
);

-- opaque collection
select YQL::RangeComputeFor(
  Struct<x:UInt32>,
  ($row) -> ($row.x IN ListFromRange(-1, 3)),
  AsTuple(AsAtom("x"))
);

-- optional collection
select YQL::RangeComputeFor(
  Struct<x:UInt32>,
  ($row) -> (($row.x IN Just(AsSet(-1, 1, 2))) ?? false),
  AsTuple(AsAtom("x"))
);

-- optional items
select YQL::RangeComputeFor(
  Struct<x:UInt32>,
  ($row) -> (($row.x IN (-1, 10u, 20, 1/0)) ?? false),
  AsTuple(AsAtom("x"))
);

-- tuple
select YQL::RangeComputeFor(
  Struct<x:UInt32, y:Uint32, z:Uint32>,
  ($row) -> (($row.y, $row.x, $row.z) IN [(1,2,3), (100,200,300)]),
  AsTuple(AsAtom("x"), AsAtom("y"), AsAtom("z"))
);


-- tuple partial
select YQL::RangeComputeFor(
  Struct<x:UInt32, y:Uint32, z:Uint32>,
  ($row) -> (($row.y, $row.x, $row.z) IN [Just(Just((1,2,3))), (100,200,300), null]),
  AsTuple(AsAtom("x"), AsAtom("y"))
);


-- tuple with implicit nulls
select YQL::RangeComputeFor(
  Struct<x:UInt32, y:Uint32, z:Uint32>,
  ($row) -> (($row.y, $row.x) IN ((1,2,3), (100, 200, 300))),
  AsTuple(AsAtom("x"), AsAtom("y"))
);

