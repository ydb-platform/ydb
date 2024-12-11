/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma warning("disable", "4510");
pragma warning("disable", "1108");
pragma AnsiInForEmptyOrNullableItemsCollections;

-- a != 2
select YQL::RangeComputeFor(
    Struct<a:PgInt4,b:PgText>,
    ($row) -> (($row.a in (3p,2p,1p)) ?? false),
    AsTuple(AsAtom("a"))
);

-- b == 'foo'
select YQL::RangeComputeFor(
    Struct<a:PgInt4,b:PgText>,
    ($row) -> (($row.b in ('foo'p, 'bar'p, 'baz'p)) ?? false),
    AsTuple(AsAtom("b"))
);

