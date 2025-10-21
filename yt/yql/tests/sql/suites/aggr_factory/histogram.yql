/* syntax version 1 */
/* postgres can not */
$t = AsList(AsStruct(1 as a),AsStruct(2 as a));
$f = AGGREGATION_FACTORY("histogram");

select Yql::Aggregate($t, AsTuple(), AsTuple(AsTuple(AsAtom("res"), $f(
    ListItemType(TypeOf($t)), ($z)->{return AsTuple($z.a,1.0)}))));

$f = AGGREGATION_FACTORY("histogram", 5);

select Yql::Aggregate($t, AsTuple(), AsTuple(AsTuple(AsAtom("res"), $f(
    ListItemType(TypeOf($t)), ($z)->{return AsTuple($z.a,1.0)}))));

use plato;
insert into @a select AsTuple(a, 1.0) as aa from as_table($t);
commit;
select AGGREGATE_BY(aa,$f) from @a;
