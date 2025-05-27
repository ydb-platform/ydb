/* syntax version 1 */
/* postgres can not */
$t = AsList(AsStruct(1 as a),AsStruct(2 as a), AsStruct(1 as a));
$f = AGGREGATION_FACTORY("avg");

use plato;
insert into @a select * from as_table($t);
commit;

select AGGREGATE_BY(distinct cast(Unicode::ToLower(cast(a as Utf8) || "00"u) as Int), $f) from @a;
