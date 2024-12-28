/* syntax version 1 */
/* postgres can not */
$t = AsList(AsStruct(1 as a),AsStruct(1 as a));
$f = AGGREGATION_FACTORY("some");

select Yql::Aggregate($t, AsTuple(), AsTuple(AsTuple(AsAtom("res"), $f(
    ListItemType(TypeOf($t)), ($z)->{return $z.a}))));

use plato;
insert into @a select * from as_table($t);
commit;
select AGGREGATE_BY(a,$f) from @a;
select AGGREGATE_BY(distinct a,$f) from @a;
