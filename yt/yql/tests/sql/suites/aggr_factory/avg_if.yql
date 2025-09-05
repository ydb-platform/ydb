/* syntax version 1 */
/* postgres can not */
$t = AsList(AsStruct(1 as a),AsStruct(2 as a));
$f = AGGREGATION_FACTORY("avg_if");

select Yql::Aggregate($t, AsTuple(), AsTuple(AsTuple(AsAtom("res"), $f(
    ListItemType(TypeOf($t)), ($z)->{return AsTuple($z.a,$z.a<2)}))));

use plato;
insert into @a select AsTuple(a,a<2) as aa from as_table($t);
commit;
select AGGREGATE_BY(aa,$f) from @a;


