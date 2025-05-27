/* syntax version 1 */
/* postgres can not */
/* yt can not */
PRAGMA warning('disable', '4510');
PRAGMA warning('disable', '1108');

-- basic in
SELECT
    YQL::RangeComputeFor(
        Struct<x: UInt32>,
        ($row) -> ($row.x IN ListFromRange(-100, 100)),
        AsTuple(AsAtom('x'))
    )
;

-- maxint
SELECT
    YQL::RangeComputeFor(
        Struct<x: Int32?>,
        ($row) -> (($row.x IN ListFromRange(2147483547ul, 2147483648ul)) ?? FALSE),
        AsTuple(AsAtom('x'))
    )
;

-- date
SELECT
    YQL::RangeComputeFor(
        Struct<x: Date>,
        ($row) -> ($row.x IN ListFromRange(Date('2105-01-01'), Date('2105-12-31')) OR $row.x == Date('2105-12-31')),
        AsTuple(AsAtom('x'))
    )
;

-- datetime
SELECT
    YQL::RangeComputeFor(
        Struct<x: Datetime>,
        ($row) -> ($row.x == Datetime('2105-12-31T23:59:58Z') OR $row.x == Datetime('2105-12-31T23:59:59Z')),
        AsTuple(AsAtom('x'))
    )
;

-- timestamp
SELECT
    YQL::RangeComputeFor(
        Struct<x: Timestamp>,
        ($row) -> ($row.x == Timestamp('2105-12-31T23:59:59.999998Z') OR $row.x == Timestamp('2105-12-31T23:59:59.999999Z')),
        AsTuple(AsAtom('x'))
    )
;
