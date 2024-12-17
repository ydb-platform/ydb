/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA warning('disable', '4510');
PRAGMA warning('disable', '1108');

-- a > 2
SELECT
    YQL::RangeComputeFor(
        Struct<a: PgInt4, b: PgText>,
        ($row) -> (FromPg(PgOp('>', $row.a, 2p)) ?? FALSE),
        AsTuple(AsAtom('a'))
    )
;

-- a >= 2
SELECT
    YQL::RangeComputeFor(
        Struct<a: PgInt4, b: PgText>,
        ($row) -> (($row.a >= 2p) ?? FALSE),
        AsTuple(AsAtom('a'))
    )
;

-- b < 2
SELECT
    YQL::RangeComputeFor(
        Struct<a: PgInt4, b: PgText>,
        ($row) -> (('2'p > $row.b) ?? FALSE),
        AsTuple(AsAtom('b'))
    )
;

-- b <= 2
SELECT
    YQL::RangeComputeFor(
        Struct<a: PgInt4, b: PgText>,
        ($row) -> (FromPg(PgOp('>=', '2'p, $row.b)) ?? FALSE),
        AsTuple(AsAtom('b'))
    )
;
