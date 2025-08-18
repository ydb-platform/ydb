/* postgres can not */
$ns = ($set)->{return ListSort(DictKeys($set))};
$nd = ($dict)->{return ListSort(DictItems($dict), ($z)->{return $z.0})};

select $ns(ToSet(AsList(1,2,3)));

select SetIsDisjoint(ToSet(AsList(1,2,3)), AsList(7,4));

select SetIsDisjoint(ToSet(AsList(1,2,3)), AsList(3,4));

select SetIsDisjoint(ToSet(AsList(1,2,3)), ToSet(AsList(7,4)));
select SetIsDisjoint(ToSet(AsList(1,2,3)), ToSet(AsList(3,4)));

select $ns(SetIntersection(ToSet(AsList(1,2,3)), ToSet(AsList(3,4))));
select $nd(SetIntersection(
    AsDict(AsTuple(1,"foo"),AsTuple(3,"bar")),
    AsDict(AsTuple(1,"baz"),AsTuple(2,"qwe")),
    ($_k, $a, $b)->{ return AsTuple($a, $b) }));

select SetIncludes(ToSet(AsList(1,2,3)), AsList(3,4));

select SetIncludes(ToSet(AsList(1,2,3)), AsList(2,3));

select SetIncludes(ToSet(AsList(1,2,3)), ToSet(AsList(3,4)));
select SetIncludes(ToSet(AsList(1,2,3)), ToSet(AsList(2,3)));

select $ns(SetDifference(ToSet(AsList(1,2,3)), ToSet(AsList(3,4))));
select $ns(SetDifference(ToSet(AsList(1,2,3)), ToSet(AsList(2,3))));

select $ns(SetUnion(ToSet(AsList(1,2,3)), ToSet(AsList(3,4))));
select $nd(SetUnion(
    AsDict(AsTuple(1,"foo"),AsTuple(3,"bar")),
    AsDict(AsTuple(1,"baz"),AsTuple(2,"qwe")),
    ($_k, $a, $b)->{ return AsTuple($a, $b) }));

select $ns(SetSymmetricDifference(ToSet(AsList(1,2,3)), ToSet(AsList(3,4))));
select $nd(SetSymmetricDifference(
    AsDict(AsTuple(1,"foo"),AsTuple(3,"bar")),
    AsDict(AsTuple(1,"baz"),AsTuple(2,"qwe")),
    ($_k, $a, $b)->{ return AsTuple($a, $b) }));
