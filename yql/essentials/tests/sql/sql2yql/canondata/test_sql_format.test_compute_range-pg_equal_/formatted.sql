/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA warning('disable', '4510');
PRAGMA warning('disable', '1108');

-- a != 2
SELECT
    YQL::RangeComputeFor(
        Struct<a: PgFloat8, b: PgText>,
        ($row) -> (FromPg(PgOp('<>', $row.a, 2.0pf8)) ?? FALSE),
        AsTuple(AsAtom('a'))
    )
;

-- b == 'foo'
SELECT
    YQL::RangeComputeFor(
        Struct<a: PgInt4, b: PgText>,
        ($row) -> (($row.b == 'foo'p) ?? FALSE),
        AsTuple(AsAtom('b'))
    )
;
