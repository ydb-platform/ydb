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
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,$i), DictLookup($d,$k);
select DictContains($d,$i), DictContains($d,$k);

$d = ToMultiDict($l);
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,$i), DictLookup($d,$k);
select DictContains($d,$i), DictContains($d,$k);

$d = Yql::ToDict($l,$first,$second,AsTuple(AsAtom("Compact"),AsAtom("Hashed"),AsAtom("One")));
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,$i), DictLookup($d,$k);
select DictContains($d,$i), DictContains($d,$k);

$d = Yql::ToDict($l,$first,$second,AsTuple(AsAtom("Compact"),AsAtom("Hashed"),AsAtom("Many")));
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,$i), DictLookup($d,$k);
select DictContains($d,$i), DictContains($d,$k);
