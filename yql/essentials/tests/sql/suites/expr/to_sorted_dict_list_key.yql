/* postgres can not */
$l = AsList(
    AsTuple(AsList(1,2,3),"foo"),
    AsTuple(AsList(1,2),"bar"),
    AsTuple(AsList(1,2),"baz")
);

$d = ToSortedDict($l);
select ListSort(DictItems($d)),DictKeys($d),DictPayloads($d);
select DictLookup($d,AsList(1,2)), DictLookup($d,AsList(1,3));
select DictContains($d,AsList(1,2)), DictContains($d,AsList(1,3));

$d = ToSortedMultiDict($l);
select ListSort(DictItems($d)),DictKeys($d),DictPayloads($d);
select DictLookup($d,AsList(1,2)), DictLookup($d,AsList(1,3));
select DictContains($d,AsList(1,2)), DictContains($d,AsList(1,3));
