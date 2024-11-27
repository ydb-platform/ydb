PRAGMA DisableSimpleColumns;
/* postgres can not */

$a = AsList(
    AsStruct(255ut AS K, 1 AS V),
    AsStruct(127ut AS K, 2 AS V),
    AsStruct(0ut   AS K, 3 AS V));

$b = AsList(
    AsStruct(Int8("-1")  AS K, 1u AS V),
    AsStruct(Int8("127") AS K, 2u AS V),
    AsStruct(Int8("0")   AS K, 3u AS V));

$aopt = AsList(
    AsStruct(Just(255ut) AS K, 1 AS V),
    AsStruct(Just(127ut) AS K, 2 AS V),
    AsStruct(Just(0ut)   AS K, 3 AS V),
    AsStruct(Nothing(ParseType("Uint8?")) AS K, 2 AS V));

$bopt = AsList(
    AsStruct(Just(Int8("-1"))  AS K, 1u AS V),
    AsStruct(Just(Int8("127")) AS K, 2u AS V),
    AsStruct(Just(Int8("0"))   AS K, 3u AS V),
    AsStruct(Nothing(ParseType("Int8?")) AS K, 2u AS V));

select a.K, b.V from as_table($a) as a
  join as_table($b) as b on a.K = b.K and a.V = b.V
order by a.K, b.V;

select a.K, b.V from as_table($aopt) as a
  join as_table($b) as b on a.K = b.K and a.V = b.V
order by a.K, b.V;

select a.K, b.V from as_table($a) as a
  join as_table($bopt) as b on a.K = b.K and a.V = b.V
order by a.K, b.V;

select a.K, b.V from as_table($aopt) as a
  join as_table($bopt) as b on a.K = b.K and a.V = b.V
order by a.K, b.V;
