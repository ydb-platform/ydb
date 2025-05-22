/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA warning('disable', '4510');
PRAGMA warning('disable', '1108');

-- like 'aaaa'
SELECT
    YQL::RangeComputeFor(
        Struct<a: PgInt4, b: PgText>,
        ($row) -> (StartsWith(FromPg($row.b), 'aaaa') ?? FALSE),
        AsTuple(AsAtom('b'))
    )
;

-- not like 'aaaa'
SELECT
    YQL::RangeComputeFor(
        Struct<a: PgInt4, b: PgText>,
        ($row) -> (NOT (StartsWith(FromPg($row.b), 'aaaa') ?? TRUE)),
        AsTuple(AsAtom('b'))
    )
;

-- like <invalid utf8>
SELECT
    YQL::RangeComputeFor(
        Struct<a: PgInt4, b: PgText>,
        ($row) -> (StartsWith(FromPg($row.b), 'a\xf5') ?? FALSE),
        AsTuple(AsAtom('b'))
    )
;

-- not like <invalid utf8>
SELECT
    YQL::RangeComputeFor(
        Struct<a: PgInt4, b: PgText>,
        ($row) -> (NOT (StartsWith(FromPg($row.b), 'a\xf5') ?? TRUE)),
        AsTuple(AsAtom('b'))
    )
;
