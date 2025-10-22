/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma warning("disable", "4510");

-- string/string
select YQL::RangeComputeFor(
  Struct<x:String>,
  ($row) -> (StartsWith($row.x, 'foo')),
  AsTuple(AsAtom("x"))
);


select YQL::RangeComputeFor(
  Struct<x:String>,
  ($row) -> (not StartsWith($row.x, 'foo')),
  AsTuple(AsAtom("x"))
);


select YQL::RangeComputeFor(
  Struct<x:String>,
  ($row) -> (StartsWith($row.x, '\xff\xff')),
  AsTuple(AsAtom("x"))
);

select YQL::RangeComputeFor(
  Struct<x:String>,
  ($row) -> (not StartsWith($row.x, '\xff\xff')),
  AsTuple(AsAtom("x"))
);

-- optional string/string
select YQL::RangeComputeFor(
  Struct<x:String?>,
  ($row) -> ((not StartsWith($row.x, 'foo')) ?? false),
  AsTuple(AsAtom("x"))
);

-- optional string/optional string
select YQL::RangeComputeFor(
  Struct<x:String?>,
  ($row) -> (StartsWith($row.x, if(1 > 2, 'void')) ?? false),
  AsTuple(AsAtom("x"))
);

--utf8/string
select YQL::RangeComputeFor(
  Struct<x:Utf8>,
  ($row) -> (StartsWith($row.x, 'тест')),
  AsTuple(AsAtom("x"))
);

select YQL::RangeComputeFor(
  Struct<x:Utf8>,
  ($row) -> (StartsWith($row.x, 'тест\xf5')),
  AsTuple(AsAtom("x"))
);

--optional utf8/utf8
select YQL::RangeComputeFor(
  Struct<x:Utf8?>,
  ($row) -> ((not StartsWith($row.x, 'тест'u)) ?? false),
  AsTuple(AsAtom("x"))
);

select YQL::RangeComputeFor(
  Struct<x:Utf8?>,
  ($row) -> (StartsWith($row.x, '\xf4\x8f\xbf\xbf'u) ?? false),
  AsTuple(AsAtom("x"))
);

-- optional utf8/string
select YQL::RangeComputeFor(
  Struct<x:Utf8?>,
  ($row) -> ((not StartsWith($row.x, 'тест\xf5')) ?? false),
  AsTuple(AsAtom("x"))
);
