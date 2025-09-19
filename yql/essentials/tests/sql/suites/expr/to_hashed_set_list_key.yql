/* postgres can not */
$first = ($x)->{return $x.0};
$second = ($x)->{return $x.1};

$l = AsList(
    AsTuple(AsList(1,2,3),Void()),
    AsTuple(AsList(1,2),Void()),
    AsTuple(AsList(1,2),Void())
);

$d = ToDict($l);
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,AsList(1,2)), DictLookup($d,AsList(1,3));
select DictContains($d,AsList(1,2)), DictContains($d,AsList(1,3));

$d = Yql::ToDict($l,$first,$second,AsTuple(AsAtom("Compact"),AsAtom("Hashed"),AsAtom("One")));
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,AsList(1,2)), DictLookup($d,AsList(1,3));
select DictContains($d,AsList(1,2)), DictContains($d,AsList(1,3));
