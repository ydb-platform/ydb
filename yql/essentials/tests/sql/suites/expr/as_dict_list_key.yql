/* postgres can not */
$d = AsDict(
    AsTuple(AsList(1,2,3),"foo"),
    AsTuple(AsList(1,2),"bar")
);

select ListSort(DictItems($d)),ListSort(DictKeys($d)),ListSort(DictPayloads($d));
select DictLookup($d,AsList(1,2)), DictLookup($d,AsList(1,3));
select DictContains($d,AsList(1,2)), DictContains($d,AsList(1,3));
