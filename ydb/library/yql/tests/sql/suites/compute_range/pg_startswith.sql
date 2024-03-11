/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma warning("disable", "4510");
pragma warning("disable", "1108");

-- like 'aaaa'
select YQL::RangeComputeFor(
	    Struct<a:PgInt4,b:PgText>,
	    ($row) -> (StartsWith(FromPg($row.b), 'aaaa') ?? false),
	    AsTuple(AsAtom("b"))
);

-- not like 'aaaa'
select YQL::RangeComputeFor(
	    Struct<a:PgInt4,b:PgText>,
	    ($row) -> (not (StartsWith(FromPg($row.b), 'aaaa') ?? true)),
	    AsTuple(AsAtom("b"))
);


-- like <invalid utf8>
select YQL::RangeComputeFor(
	    Struct<a:PgInt4,b:PgText>,
	    ($row) -> (StartsWith(FromPg($row.b), 'a\xf5') ?? false),
	    AsTuple(AsAtom("b"))
);

-- not like <invalid utf8>
select YQL::RangeComputeFor(
	    Struct<a:PgInt4,b:PgText>,
	    ($row) -> (not (StartsWith(FromPg($row.b), 'a\xf5') ?? true)),
	    AsTuple(AsAtom("b"))
);
