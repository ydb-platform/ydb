/* postgres can not */
$d = AsDict(
    AsTuple(AsTuple(),"foo"),
    AsTuple(AsTuple(),"bar")
);

select ListSort(DictItems($d)),ListSort(DictKeys($d)),ListSort(DictPayloads($d));
select DictLookup($d,AsTuple());
select DictContains($d,AsTuple());

$d = AsDict(
    AsTuple(AsTuple(1),"foo"),
    AsTuple(AsTuple(2),"bar")
);

select ListSort(DictItems($d)),ListSort(DictKeys($d)),ListSort(DictPayloads($d));
select DictLookup($d,AsTuple(1)),DictLookup($d,AsTuple(3));
select DictContains($d,AsTuple(1)),DictContains($d,AsTuple(3));

$d = AsDict(
    AsTuple(AsTuple(1,2),"foo"),
    AsTuple(AsTuple(1,3),"bar")
);

select ListSort(DictItems($d)),ListSort(DictKeys($d)),ListSort(DictPayloads($d));
select DictLookup($d,AsTuple(1,2)),DictLookup($d,AsTuple(1,4));
select DictContains($d,AsTuple(1,2)),DictContains($d,AsTuple(1,4));
