/* postgres can not */
$first = ($x)->{return $x.0};
$second = ($x)->{return $x.1};

$i = AsDict(AsTuple(1,"A"),AsTuple(2,"B"));
$j = AsDict(AsTuple(1,"A"),AsTuple(2,"C"));
$k = AsDict(AsTuple(1,"A"),AsTuple(2,"D"));

$l = AsList(
    AsTuple($i,"foo"),
    AsTuple($i,"bar"),
    AsTuple($j,"baz")
);

$d = ToDict($l);
select ListSort(ListFlatten(ListMap(DictItems($d), ($x) -> {return ListMap(DictItems($x.0), ($y) -> {return ($y, $x.1)})}))),
    ListSort(ListFlatten(ListMap(DictKeys($d), ($x) -> {return DictItems($x)}))),
    ListSort(DictPayloads($d));
select DictLookup($d,$i), DictLookup($d,$k);
select DictContains($d,$i), DictContains($d,$k);

$d = ToMultiDict($l);
select ListSort(ListFlatten(ListMap(DictItems($d), ($x) -> {return ListMap(DictItems($x.0), ($y) -> {return ($y, $x.1)})}))),
    ListSort(ListFlatten(ListMap(DictKeys($d), ($x) -> {return DictItems($x)}))),
    ListSort(DictPayloads($d));
select DictLookup($d,$i), DictLookup($d,$k);
select DictContains($d,$i), DictContains($d,$k);

$d = Yql::ToDict($l,$first,$second,AsTuple(AsAtom("Compact"),AsAtom("Hashed"),AsAtom("One")));
select ListSort(ListFlatten(ListMap(DictItems($d), ($x) -> {return ListMap(DictItems($x.0), ($y) -> {return ($y, $x.1)})}))),
    ListSort(ListFlatten(ListMap(DictKeys($d), ($x) -> {return DictItems($x)}))),
    ListSort(DictPayloads($d));
select DictLookup($d,$i), DictLookup($d,$k);
select DictContains($d,$i), DictContains($d,$k);

$d = Yql::ToDict($l,$first,$second,AsTuple(AsAtom("Compact"),AsAtom("Hashed"),AsAtom("Many")));
select ListSort(ListFlatten(ListMap(DictItems($d), ($x) -> {return ListMap(DictItems($x.0), ($y) -> {return ($y, $x.1)})}))),
    ListSort(ListFlatten(ListMap(DictKeys($d), ($x) -> {return DictItems($x)}))),
    ListSort(DictPayloads($d));
select DictLookup($d,$i), DictLookup($d,$k);
