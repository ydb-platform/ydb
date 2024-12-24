/* postgres can not */
$first = ($x) -> {
    RETURN $x.0;
};

$second = ($x) -> {
    RETURN $x.1;
};

$vt = ParseType('Variant<Int32,Uint32>');
$v1 = VARIANT (1, '0', $vt);
$v2 = VARIANT (2u, '1', $vt);
$v3 = VARIANT (2, '0', $vt);

$l = AsList(
    AsTuple($v1, 'foo'),
    AsTuple($v2, 'bar'),
    AsTuple($v2, 'baz')
);

$d = ToDict($l);

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, $v1),
    DictLookup($d, $v3)
;

SELECT
    DictContains($d, $v1),
    DictContains($d, $v3)
;

$d = ToMultiDict($l);

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
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
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, $v1),
    DictLookup($d, $v3)
;

SELECT
    DictContains($d, $v1),
    DictContains($d, $v3)
;

$d = Yql::ToDict($l, $first, $second, AsTuple(AsAtom('Compact'), AsAtom('Hashed'), AsAtom('Many')));

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, $v1),
    DictLookup($d, $v3)
;

SELECT
    DictContains($d, $v1),
    DictContains($d, $v3)
;
