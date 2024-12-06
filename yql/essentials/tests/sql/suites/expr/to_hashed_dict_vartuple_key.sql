/* postgres can not */
$first = ($x)->{return $x.0};
$second = ($x)->{return $x.1};

$vt = ParseType("Variant<Int32,Uint32>");
$v1 = Variant(1,"0",$vt);
$v2 = Variant(2u,"1",$vt);
$v3 = Variant(2,"0",$vt);

$l = AsList(
    AsTuple($v1,"foo"),
    AsTuple($v2,"bar"),
    AsTuple($v2,"baz")
);

$d = ToDict($l);
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,$v1), DictLookup($d,$v3);
select DictContains($d,$v1), DictContains($d,$v3);

$d = ToMultiDict($l);
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,$v1), DictLookup($d,$v3);
select DictContains($d,$v1), DictContains($d,$v3);

$d = Yql::ToDict($l,$first,$second,AsTuple(AsAtom("Compact"),AsAtom("Hashed"),AsAtom("One")));
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,$v1), DictLookup($d,$v3);
select DictContains($d,$v1), DictContains($d,$v3);

$d = Yql::ToDict($l,$first,$second,AsTuple(AsAtom("Compact"),AsAtom("Hashed"),AsAtom("Many")));
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,$v1), DictLookup($d,$v3);
select DictContains($d,$v1), DictContains($d,$v3);
