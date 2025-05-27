/* postgres can not */
$first = ($x) -> {
    RETURN $x.0;
};

$second = ($x) -> {
    RETURN $x.1;
};

$l = AsList(
    AsTuple(AsTuple(), 'foo'),
    AsTuple(AsTuple(), 'bar'),
    AsTuple(AsTuple(), 'baz')
);

$d = ToDict($l);

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, AsTuple())
;

SELECT
    DictContains($d, AsTuple())
;

$d = ToMultiDict($l);

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, AsTuple())
;

SELECT
    DictContains($d, AsTuple())
;

$d = Yql::ToDict($l, $first, $second, AsTuple(AsAtom('Compact'), AsAtom('Hashed'), AsAtom('One')));

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, AsTuple())
;

SELECT
    DictContains($d, AsTuple())
;

$d = Yql::ToDict($l, $first, $second, AsTuple(AsAtom('Compact'), AsAtom('Hashed'), AsAtom('Many')));

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, AsTuple())
;

SELECT
    DictContains($d, AsTuple())
;

$l = AsList(
    AsTuple(AsTuple(1), 'foo'),
    AsTuple(AsTuple(2), 'bar'),
    AsTuple(AsTuple(2), 'baz')
);

$d = ToDict($l);

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, AsTuple(2)),
    DictLookup($d, AsTuple(3))
;

SELECT
    DictContains($d, AsTuple(2)),
    DictContains($d, AsTuple(3))
;

$d = ToMultiDict($l);

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, AsTuple(2)),
    DictLookup($d, AsTuple(3))
;

SELECT
    DictContains($d, AsTuple(2)),
    DictContains($d, AsTuple(3))
;

$d = Yql::ToDict($l, $first, $second, AsTuple(AsAtom('Compact'), AsAtom('Hashed'), AsAtom('One')));

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, AsTuple(2)),
    DictLookup($d, AsTuple(3))
;

SELECT
    DictContains($d, AsTuple(2)),
    DictContains($d, AsTuple(3))
;

$d = Yql::ToDict($l, $first, $second, AsTuple(AsAtom('Compact'), AsAtom('Hashed'), AsAtom('Many')));

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, AsTuple(2)),
    DictLookup($d, AsTuple(3))
;

SELECT
    DictContains($d, AsTuple(2)),
    DictContains($d, AsTuple(3))
;

$l = AsList(
    AsTuple(AsTuple(1, 2), 'foo'),
    AsTuple(AsTuple(1, 3), 'bar'),
    AsTuple(AsTuple(1, 3), 'baz')
);

$d = ToDict($l);

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, AsTuple(1, 2)),
    DictLookup($d, AsTuple(1, 4))
;

SELECT
    DictContains($d, AsTuple(1, 2)),
    DictContains($d, AsTuple(1, 4))
;

$d = ToMultiDict($l);

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, AsTuple(1, 2)),
    DictLookup($d, AsTuple(1, 4))
;

SELECT
    DictContains($d, AsTuple(1, 2)),
    DictContains($d, AsTuple(1, 4))
;

$d = Yql::ToDict($l, $first, $second, AsTuple(AsAtom('Compact'), AsAtom('Hashed'), AsAtom('One')));

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, AsTuple(1, 2)),
    DictLookup($d, AsTuple(1, 4))
;

SELECT
    DictContains($d, AsTuple(1, 2)),
    DictContains($d, AsTuple(1, 4))
;

$d = Yql::ToDict($l, $first, $second, AsTuple(AsAtom('Compact'), AsAtom('Hashed'), AsAtom('Many')));

SELECT
    DictKeys($d),
    DictPayloads($d),
    DictItems($d)
;

SELECT
    DictLookup($d, AsTuple(1, 2)),
    DictLookup($d, AsTuple(1, 4))
;

SELECT
    DictContains($d, AsTuple(1, 2)),
    DictContains($d, AsTuple(1, 4))
;
