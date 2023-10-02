/* postgres can not */
$d1 = AsDict(
    AsTuple(AsList(1,2,3),"foo"),
    AsTuple(AsList(1,2),"bar")
);


$d2 = AsDict(
    AsTuple(AsList(1,3),"baz"),
    AsTuple(AsList(1,2),"qwe")
);

$d3 = DictCreate(DictKeyType(TypeOf($d2)), DictPayloadType(TypeOf($d2)));

$d = AsDict(
    AsTuple($d1, 17),
    AsTuple($d2, 32)
);

select $d,DictKeys($d),DictPayloads($d),DictItems($d);
select DictLookup($d,$d1), DictLookup($d,$d3);
select DictContains($d,$d1), DictContains($d,$d3);
