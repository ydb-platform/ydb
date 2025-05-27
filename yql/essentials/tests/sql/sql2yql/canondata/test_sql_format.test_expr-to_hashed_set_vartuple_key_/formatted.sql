/* postgres can not */
$first = ($x) -> {
    RETURN $x.0;
};

$second = ($x) -> {
    RETURN $x.1;
};

$vt = ParseType('Variant<Int32,Uint32>');
$v1 = Variant(1, '0', $vt);
$v2 = Variant(2u, '1', $vt);
$v3 = Variant(2, '0', $vt);

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
