/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA warning('disable', '4510');
PRAGMA warning('disable', '1108');
PRAGMA AnsiInForEmptyOrNullableItemsCollections;

-- a != 2
SELECT
    YQL::RangeComputeFor(
        Struct<a: PgInt4, b: PgText>,
        ($row) -> (($row.a IN (3p, 2p, 1p)) ?? FALSE),
        AsTuple(AsAtom('a'))
    )
;

-- b == 'foo'
SELECT
    YQL::RangeComputeFor(
        Struct<a: PgInt4, b: PgText>,
        ($row) -> (($row.b IN ('foo'p, 'bar'p, 'baz'p)) ?? FALSE),
        AsTuple(AsAtom('b'))
    )
;
