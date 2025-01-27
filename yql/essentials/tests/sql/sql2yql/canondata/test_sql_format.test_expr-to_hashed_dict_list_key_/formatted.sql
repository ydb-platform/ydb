/* postgres can not */
$first = ($x) -> {
    RETURN $x.0;
};

$second = ($x) -> {
    RETURN $x.1;
};

$l = AsList(
    AsTuple(AsList(1, 2, 3), 'foo'),
    AsTuple(AsList(1, 2), 'bar'),
    AsTuple(AsList(1, 2), 'baz')
);

$d = ToDict($l);

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, AsList(1, 2)),
    DictLookup($d, AsList(1, 3))
;

SELECT
    DictContains($d, AsList(1, 2)),
    DictContains($d, AsList(1, 3))
;

$d = ToMultiDict($l);

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, AsList(1, 2)),
    DictLookup($d, AsList(1, 3))
;

SELECT
    DictContains($d, AsList(1, 2)),
    DictContains($d, AsList(1, 3))
;

$d = Yql::ToDict($l, $first, $second, AsTuple(AsAtom('Compact'), AsAtom('Hashed'), AsAtom('One')));

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, AsList(1, 2)),
    DictLookup($d, AsList(1, 3))
;

SELECT
    DictContains($d, AsList(1, 2)),
    DictContains($d, AsList(1, 3))
;

$d = Yql::ToDict($l, $first, $second, AsTuple(AsAtom('Compact'), AsAtom('Hashed'), AsAtom('Many')));

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, AsList(1, 2)),
    DictLookup($d, AsList(1, 3))
;

SELECT
    DictContains($d, AsList(1, 2)),
    DictContains($d, AsList(1, 3))
;
