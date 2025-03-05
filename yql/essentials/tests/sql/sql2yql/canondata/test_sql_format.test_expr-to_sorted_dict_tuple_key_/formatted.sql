/* postgres can not */
$l = AsList(
    AsTuple(AsTuple(), 'foo'),
    AsTuple(AsTuple(), 'bar'),
    AsTuple(AsTuple(), 'baz')
);

$d = ToSortedDict($l);

SELECT
    ListSort(DictItems($d)),
    ListSort(DictKeys($d)),
    ListSort(DictPayloads($d))
;

SELECT
    DictLookup($d, AsTuple())
;

SELECT
    DictContains($d, AsTuple())
;

$d = ToSortedMultiDict($l);

SELECT
    ListSort(DictItems($d)),
    ListSort(DictKeys($d)),
    ListSort(DictPayloads($d))
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

$d = ToSortedDict($l);

SELECT
    ListSort(DictItems($d)),
    ListSort(DictKeys($d)),
    ListSort(DictPayloads($d))
;

SELECT
    DictLookup($d, AsTuple(2)),
    DictLookup($d, AsTuple(3))
;

SELECT
    DictContains($d, AsTuple(2)),
    DictContains($d, AsTuple(3))
;

$d = ToSortedMultiDict($l);

SELECT
    ListSort(DictItems($d)),
    ListSort(DictKeys($d)),
    ListSort(DictPayloads($d))
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

$d = ToSortedDict($l);

SELECT
    ListSort(DictItems($d)),
    ListSort(DictKeys($d)),
    ListSort(DictPayloads($d))
;

SELECT
    DictLookup($d, AsTuple(1, 2)),
    DictLookup($d, AsTuple(1, 4))
;

SELECT
    DictContains($d, AsTuple(1, 2)),
    DictContains($d, AsTuple(1, 4))
;

$d = ToSortedMultiDict($l);

SELECT
    ListSort(DictItems($d)),
    ListSort(DictKeys($d)),
    ListSort(DictPayloads($d))
;

SELECT
    DictLookup($d, AsTuple(1, 2)),
    DictLookup($d, AsTuple(1, 4))
;

SELECT
    DictContains($d, AsTuple(1, 2)),
    DictContains($d, AsTuple(1, 4))
;
