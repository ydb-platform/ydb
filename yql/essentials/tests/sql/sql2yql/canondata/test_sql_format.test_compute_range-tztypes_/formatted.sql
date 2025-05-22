/* syntax version 1 */
/* postgres can not */
/* yt can not */
PRAGMA warning('disable', '4510');

-- ==
SELECT
    YQL::RangeComputeFor(
        Struct<x: TzDate>,
        ($row) -> ($row.x == TzDate('2000-01-01,Europe/Moscow')),
        AsTuple(AsAtom('x'))
    )
;

-- !=
SELECT
    YQL::RangeComputeFor(
        Struct<x: TzDate>,
        ($row) -> ($row.x != TzDate('2000-01-01,Europe/Moscow')),
        AsTuple(AsAtom('x'))
    )
;

-- >
SELECT
    YQL::RangeComputeFor(
        Struct<x: TzDatetime>,
        ($row) -> ($row.x > TzDatetime('2000-01-01T00:00:00,Europe/Moscow')),
        AsTuple(AsAtom('x'))
    )
;

-- >=
SELECT
    YQL::RangeComputeFor(
        Struct<x: TzDatetime>,
        ($row) -> ($row.x >= TzDatetime('2000-01-01T00:00:00,Europe/Moscow')),
        AsTuple(AsAtom('x'))
    )
;

-- <
SELECT
    YQL::RangeComputeFor(
        Struct<x: TzTimestamp>,
        ($row) -> ($row.x < TzTimestamp('2000-01-01T00:00:00.000000,Europe/Moscow')),
        AsTuple(AsAtom('x'))
    )
;

-- <=
SELECT
    YQL::RangeComputeFor(
        Struct<x: TzTimestamp>,
        ($row) -> ($row.x <= TzTimestamp('2000-01-01T00:00:00.000000,Europe/Moscow')),
        AsTuple(AsAtom('x'))
    )
;
