/* postgres can not */
$first = ($x)->{return $x.0};
$second = ($x)->{return $x.1};

$vt = ParseType("Variant<a:Int32,b:Uint32>");
$v1 = Variant(1,"a",$vt);
$v2 = Variant(2u,"b",$vt);
$v3 = Variant(2,"a",$vt);

$l = AsList(
    AsTuple($v1,Void()),
    AsTuple($v2,Void()),
    AsTuple($v2,Void())
);

$d = ToDict($l);
select ListSort(DictItems($d)),ListSort(DictKeys($d)),ListSort(DictPayloads($d));
select DictLookup($d,$v1), DictLookup($d,$v3);
select DictContains($d,$v1), DictContains($d,$v3);

$d = Yql::ToDict($l,$first,$second,AsTuple(AsAtom("Compact"),AsAtom("Hashed"),AsAtom("One")));
select ListSort(DictItems($d)),ListSort(DictKeys($d)),ListSort(DictPayloads($d));
select DictLookup($d,$v1), DictLookup($d,$v3);
select DictContains($d,$v1), DictContains($d,$v3);
