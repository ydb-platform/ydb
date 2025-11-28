/* syntax version 1 */
/* postgres can not */
/* yt can not */

pragma warning("disable", "4510");

-- ==
select YQL::RangeComputeFor(
  Struct<x:TzDate>,
  ($row) -> ($row.x == TzDate('2000-01-01,Europe/Moscow')),
  AsTuple(AsAtom("x"))
);

-- !=
select YQL::RangeComputeFor(
  Struct<x:TzDate>,
  ($row) -> ($row.x != TzDate('2000-01-01,Europe/Moscow')),
  AsTuple(AsAtom("x"))
);

-- >
select YQL::RangeComputeFor(
  Struct<x:TzDatetime>,
  ($row) -> ($row.x > TzDatetime('2000-01-01T00:00:00,Europe/Moscow')),
  AsTuple(AsAtom("x"))
);

-- >=
select YQL::RangeComputeFor(
  Struct<x:TzDatetime>,
  ($row) -> ($row.x >= TzDatetime('2000-01-01T00:00:00,Europe/Moscow')),
  AsTuple(AsAtom("x"))
);

-- <
select YQL::RangeComputeFor(
  Struct<x:TzTimestamp>,
  ($row) -> ($row.x < TzTimestamp('2000-01-01T00:00:00.000000,Europe/Moscow')),
  AsTuple(AsAtom("x"))
);

-- <=
select YQL::RangeComputeFor(
  Struct<x:TzTimestamp>,
  ($row) -> ($row.x <= TzTimestamp('2000-01-01T00:00:00.000000,Europe/Moscow')),
  AsTuple(AsAtom("x"))
);

