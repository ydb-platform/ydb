/* syntax version 1 */
/* postgres can not */
$t = AsList(
    AsStruct(1 as key, 101 as value),
    AsStruct(6 as key, 34 as value),
    AsStruct(4 as key, 22 as value),
    AsStruct(2 as key, 256 as value),
    AsStruct(7 as key, 111 as value)
);

$f = AGGREGATION_FACTORY("topby", 3);

select Yql::Aggregate($t, AsTuple(), AsTuple(AsTuple(AsAtom("res"),
    $f(ListItemType(TypeOf($t)), ($z)->{ return AsTuple($z.value, $z.key) }))));

use plato;
insert into @a select AsTuple(value, key) as vk from as_table($t);
commit;

select AGGREGATE_BY(vk, $f) from @a;

