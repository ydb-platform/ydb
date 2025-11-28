/* postgres can not */

$l = AsList(
    AsTuple(AsList(1,2,3),Void()),
    AsTuple(AsList(1,2),Void()),
    AsTuple(AsList(1,2),Void())
);

$d = ToSortedDict($l);
select ListSort(DictItems($d)),ListSort(DictKeys($d)),ListSort(DictPayloads($d));
select DictLookup($d,AsList(1,2)), DictLookup($d,AsList(1,3));
select DictContains($d,AsList(1,2)), DictContains($d,AsList(1,3));
