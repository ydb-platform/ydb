/* syntax version 1 */
/* postgres can not */
$t = AsList(AsStruct(1 as a),AsStruct(2 as a));
$f = AGGREGATION_FACTORY("aggregate_list");

select Yql::Aggregate($t, AsTuple(), AsTuple(AsTuple(AsAtom("res"), $f(
    ListItemType(TypeOf($t)), ($z)->{return $z.a}))));

$f = AGGREGATION_FACTORY("aggregate_list", length(CAST(Unicode::ToUpper("xx"u) AS String)));

select Yql::Aggregate($t, AsTuple(), AsTuple(AsTuple(AsAtom("res"), $f(
    ListItemType(TypeOf($t)), ($z)->{return $z.a}))));

use plato;
insert into @a select * from as_table($t);
commit;
select ListSort(AGGREGATE_BY(a,$f)) from @a;
select ListSort(AGGREGATE_BY(distinct a,$f)) from @a;
