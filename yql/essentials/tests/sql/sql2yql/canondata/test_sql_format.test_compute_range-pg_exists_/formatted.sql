/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA warning('disable', '4510');
PRAGMA warning('disable', '1108');

SELECT
    YQL::RangeComputeFor(
        Struct<a: PgFloat8, b: PgText>,
        ($row) -> ($row.a IS NOT NULL),
        AsTuple(AsAtom('a'))
    )
;

SELECT
    YQL::RangeComputeFor(
        Struct<a: PgInt4, b: PgText>,
        ($row) -> ($row.b IS NULL),
        AsTuple(AsAtom('b'))
    )
;
