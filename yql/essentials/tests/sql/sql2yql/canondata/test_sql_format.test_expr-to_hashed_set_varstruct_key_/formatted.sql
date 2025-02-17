/* postgres can not */
$first = ($x) -> {
    RETURN $x.0;
};

$second = ($x) -> {
    RETURN $x.1;
};

$vt = ParseType('Variant<a:Int32,b:Uint32>');
$v1 = VARIANT (1, 'a', $vt);
$v2 = VARIANT (2u, 'b', $vt);
$v3 = VARIANT (2, 'a', $vt);

$l = AsList(
    AsTuple($v1, Void()),
    AsTuple($v2, Void()),
    AsTuple($v2, Void())
);

$d = ToDict($l);

SELECT
    ListSort(DictItems($d)),
    ListSort(DictKeys($d)),
    ListSort(DictPayloads($d))
;

SELECT
    DictLookup($d, $v1),
    DictLookup($d, $v3)
;

SELECT
    DictContains($d, $v1),
    DictContains($d, $v3)
;

$d = Yql::ToDict($l, $first, $second, AsTuple(AsAtom('Compact'), AsAtom('Hashed'), AsAtom('One')));

SELECT
    ListSort(DictItems($d)),
    ListSort(DictKeys($d)),
    ListSort(DictPayloads($d))
;

SELECT
    DictLookup($d, $v1),
    DictLookup($d, $v3)
;

SELECT
    DictContains($d, $v1),
    DictContains($d, $v3)
;
