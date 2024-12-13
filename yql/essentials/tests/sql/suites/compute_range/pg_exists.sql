/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma warning("disable", "4510");
pragma warning("disable", "1108");

select YQL::RangeComputeFor(
    Struct<a:PgFloat8,b:PgText>,
    ($row) -> ($row.a is not null),
    AsTuple(AsAtom("a"))
);

select YQL::RangeComputeFor(
    Struct<a:PgInt4,b:PgText>,
    ($row) -> ($row.b is null),
    AsTuple(AsAtom("b"))
);

