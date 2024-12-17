/* syntax version 1 */
/* postgres can not */
/* yt can not */

pragma warning("disable", "4510");

-- [10, 11) -> [10, 10]
select YQL::RangeComputeFor(
  Struct<x:UInt32>,
  ($row) -> ($row.x >= 10 and $row.x < 11),
  AsTuple(AsAtom("x"))
);

-- (10, 11] -> [11, 11]
select YQL::RangeComputeFor(
  Struct<x:UInt32>,
  ($row) -> ($row.x > 10 and $row.x <= 11),
  AsTuple(AsAtom("x"))
);

-- dates
select YQL::RangeComputeFor(
  Struct<x:Date??>,
  ($row) -> (($row.x > Date("2021-09-08") and $row.x <= Date("2021-09-09")) ?? false),
  AsTuple(AsAtom("x"))
)
, YQL::RangeComputeFor(
  Struct<x:Date32??>,
  ($row) -> (($row.x > Date("2021-09-08") and $row.x <= Date("2021-09-09")) ?? false),
  AsTuple(AsAtom("x"))
)
, YQL::RangeComputeFor(
  Struct<x:Date??>,
  ($row) -> (($row.x > Date32("2021-09-08") and $row.x <= Date32("2021-09-09")) ?? false),
  AsTuple(AsAtom("x"))
)
, YQL::RangeComputeFor(
  Struct<x:Date32??>,
  ($row) -> (($row.x > Date32("-1-12-31") and $row.x <= Date32("1-01-01")) ?? false),
  AsTuple(AsAtom("x"))
);

-- datetimes
select YQL::RangeComputeFor(
  Struct<x:Datetime?>,
  ($row) -> (($row.x > Datetime("2021-09-09T12:00:00Z") and $row.x <= Datetime("2021-09-09T12:00:01Z")) ?? false),
  AsTuple(AsAtom("x"))
)
, YQL::RangeComputeFor(
  Struct<x:Datetime64?>,
  ($row) -> (($row.x > Datetime("2021-09-09T12:00:00Z") and $row.x <= Datetime("2021-09-09T12:00:01Z")) ?? false),
  AsTuple(AsAtom("x"))
)
, YQL::RangeComputeFor(
  Struct<x:Datetime?>,
  ($row) -> (($row.x > Datetime64("2021-09-09T12:00:00Z") and $row.x <= Datetime64("2021-09-09T12:00:01Z")) ?? false),
  AsTuple(AsAtom("x"))
)
, YQL::RangeComputeFor(
  Struct<x:Datetime64?>,
  ($row) -> (($row.x > Datetime64("-1-12-31T23:59:59Z") and $row.x <= Datetime64("1-01-01T00:00:00Z")) ?? false),
  AsTuple(AsAtom("x"))
);

-- timestamps
select YQL::RangeComputeFor(
  Struct<x:Timestamp??>,
  ($row) -> (($row.x > Timestamp("2021-09-09T12:00:00.000000Z") and $row.x <= Timestamp("2021-09-09T12:00:00.000001Z")) ?? false),
  AsTuple(AsAtom("x"))
)
, YQL::RangeComputeFor(
  Struct<x:Timestamp64??>,
  ($row) -> (($row.x > Timestamp("2021-09-09T12:00:00.000000Z") and $row.x <= Timestamp("2021-09-09T12:00:00.000001Z")) ?? false),
  AsTuple(AsAtom("x"))
)
, YQL::RangeComputeFor(
  Struct<x:Timestamp??>,
  ($row) -> (($row.x > Timestamp64("2021-09-09T12:00:00.000000Z") and $row.x <= Timestamp64("2021-09-09T12:00:00.000001Z")) ?? false),
  AsTuple(AsAtom("x"))
)
, YQL::RangeComputeFor(
  Struct<x:Timestamp64??>,
  ($row) -> (($row.x > Timestamp64("-1-12-31T23:59:59.999999Z") and $row.x <= Timestamp64("1-01-01T00:00:00.000000Z")) ?? false),
  AsTuple(AsAtom("x"))
);
