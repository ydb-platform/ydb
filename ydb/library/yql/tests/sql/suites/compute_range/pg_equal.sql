/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma warning("disable", "4510");
pragma warning("disable", "1108");

-- a != 2
select YQL::RangeComputeFor(
    Struct<a:PgFloat8,b:PgText>,
    ($row) -> (FromPg(PgOp("<>", $row.a, 2.0pf8)) ?? false),
    AsTuple(AsAtom("a"))
);

-- b == 'foo'
select YQL::RangeComputeFor(
    Struct<a:PgInt4,b:PgText>,
    ($row) -> (($row.b == 'foo'p) ?? false),
    AsTuple(AsAtom("b"))
);

