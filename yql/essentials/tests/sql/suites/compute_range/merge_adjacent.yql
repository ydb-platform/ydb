/* syntax version 1 */
/* postgres can not */
/* yt can not */

pragma warning("disable", "4510");
pragma warning("disable", "1108");

-- basic in
select YQL::RangeComputeFor(
  Struct<x:UInt32>,
  ($row) -> ($row.x in ListFromRange(-100, 100)),
  AsTuple(AsAtom("x"))
);

-- maxint
select YQL::RangeComputeFor(
  Struct<x:Int32?>,
  ($row) -> (($row.x in ListFromRange(2147483547ul, 2147483648ul)) ?? false),
  AsTuple(AsAtom("x"))
);

-- date
select YQL::RangeComputeFor(
  Struct<x:Date>,
  ($row) -> ($row.x in ListFromRange(Date("2105-01-01"), Date("2105-12-31")) or $row.x == Date("2105-12-31")),
  AsTuple(AsAtom("x"))
);

-- datetime
select YQL::RangeComputeFor(
  Struct<x:Datetime>,
  ($row) -> ($row.x == Datetime("2105-12-31T23:59:58Z") or $row.x == Datetime("2105-12-31T23:59:59Z")),
  AsTuple(AsAtom("x"))
);


-- timestamp
select YQL::RangeComputeFor(
  Struct<x:Timestamp>,
  ($row) -> ($row.x == Timestamp("2105-12-31T23:59:59.999998Z") or $row.x == Timestamp("2105-12-31T23:59:59.999999Z")),
  AsTuple(AsAtom("x"))
);

