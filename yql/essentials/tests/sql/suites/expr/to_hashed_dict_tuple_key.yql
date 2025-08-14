/* postgres can not */
$first = ($x)->{return $x.0};
$second = ($x)->{return $x.1};

$l = AsList(
    AsTuple(AsTuple(),"foo"),
    AsTuple(AsTuple(),"bar"),
    AsTuple(AsTuple(),"baz")
);

$d = ToDict($l);
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,AsTuple());
select DictContains($d,AsTuple());

$d = ToMultiDict($l);
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,AsTuple());
select DictContains($d,AsTuple());

$d = Yql::ToDict($l,$first,$second,AsTuple(AsAtom("Compact"),AsAtom("Hashed"),AsAtom("One")));
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,AsTuple());
select DictContains($d,AsTuple());

$d = Yql::ToDict($l,$first,$second,AsTuple(AsAtom("Compact"),AsAtom("Hashed"),AsAtom("Many")));
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,AsTuple());
select DictContains($d,AsTuple());

$l = AsList(
    AsTuple(AsTuple(1),"foo"),
    AsTuple(AsTuple(2),"bar"),
    AsTuple(AsTuple(2),"baz")
);

$d = ToDict($l);
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,AsTuple(2)), DictLookup($d,AsTuple(3));
select DictContains($d,AsTuple(2)), DictContains($d,AsTuple(3));

$d = ToMultiDict($l);
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,AsTuple(2)), DictLookup($d,AsTuple(3));
select DictContains($d,AsTuple(2)), DictContains($d,AsTuple(3));

$d = Yql::ToDict($l,$first,$second,AsTuple(AsAtom("Compact"),AsAtom("Hashed"),AsAtom("One")));
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,AsTuple(2)), DictLookup($d,AsTuple(3));
select DictContains($d,AsTuple(2)), DictContains($d,AsTuple(3));

$d = Yql::ToDict($l,$first,$second,AsTuple(AsAtom("Compact"),AsAtom("Hashed"),AsAtom("Many")));
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,AsTuple(2)), DictLookup($d,AsTuple(3));
select DictContains($d,AsTuple(2)), DictContains($d,AsTuple(3));

$l = AsList(
    AsTuple(AsTuple(1,2),"foo"),
    AsTuple(AsTuple(1,3),"bar"),
    AsTuple(AsTuple(1,3),"baz")
);

$d = ToDict($l);
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,AsTuple(1,2)), DictLookup($d,AsTuple(1,4));
select DictContains($d,AsTuple(1,2)), DictContains($d,AsTuple(1,4));

$d = ToMultiDict($l);
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,AsTuple(1,2)), DictLookup($d,AsTuple(1,4));
select DictContains($d,AsTuple(1,2)), DictContains($d,AsTuple(1,4));

$d = Yql::ToDict($l,$first,$second,AsTuple(AsAtom("Compact"),AsAtom("Hashed"),AsAtom("One")));
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,AsTuple(1,2)), DictLookup($d,AsTuple(1,4));
select DictContains($d,AsTuple(1,2)), DictContains($d,AsTuple(1,4));

$d = Yql::ToDict($l,$first,$second,AsTuple(AsAtom("Compact"),AsAtom("Hashed"),AsAtom("Many")));
select DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,AsTuple(1,2)), DictLookup($d,AsTuple(1,4));
select DictContains($d,AsTuple(1,2)), DictContains($d,AsTuple(1,4));
