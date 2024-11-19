/* syntax version 1 */
/* postgres can not */
$t = AsList(
    AsStruct(1 as key, 200 as value),
    AsStruct(2 as key, 100 as value)
);

$f = AGGREGATION_FACTORY("minby");

select Yql::Aggregate($t, AsTuple(), AsTuple(AsTuple(AsAtom("res"),
    $f(ListItemType(TypeOf($t)), ($z)->{ return AsTuple($z.value, $z.key) }))));

$f = AGGREGATION_FACTORY("minby", 10);

select Yql::Aggregate($t, AsTuple(), AsTuple(AsTuple(AsAtom("res"),
    $f(ListItemType(TypeOf($t)), ($z)->{ return AsTuple($z.value, $z.key) }))));

use plato;
insert into @a select AsTuple(value, key) as vk from as_table($t);
commit;
select AGGREGATE_BY(vk, $f) from @a;
