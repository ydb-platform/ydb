/* syntax version 1 */
/* postgres can not */
$t = AsList(AsStruct(1 as a),AsStruct(2 as a));
$f = AGGREGATION_FACTORY("mode");

select Yql::Aggregate($t, AsTuple(), AsTuple(AsTuple(AsAtom("res"), $f(
    ListItemType(TypeOf($t)), ($z)->{return $z.a}))));

$f = AGGREGATION_FACTORY("topfreq", 10, 20);

select Yql::Aggregate($t, AsTuple(), AsTuple(AsTuple(AsAtom("res"), $f(
    ListItemType(TypeOf($t)), ($z)->{return $z.a}))));

use plato;
insert into @a select * from as_table($t);
commit;
select AGGREGATE_BY(a,$f) from @a;
select ListSort(AGGREGATE_BY(distinct a,$f), ($x)->{ return $x.Value }) from @a;
