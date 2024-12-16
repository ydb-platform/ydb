/* postgres can not */
$d = AsDict(
    AsTuple(AsTuple(), 'foo'),
    AsTuple(AsTuple(), 'bar')
);

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

$d = AsDict(
    AsTuple(AsTuple(1), 'foo'),
    AsTuple(AsTuple(2), 'bar')
);

SELECT
    ListSort(DictItems($d)),
    ListSort(DictKeys($d)),
    ListSort(DictPayloads($d))
;

SELECT
    DictLookup($d, AsTuple(1)),
    DictLookup($d, AsTuple(3))
;

SELECT
    DictContains($d, AsTuple(1)),
    DictContains($d, AsTuple(3))
;

$d = AsDict(
    AsTuple(AsTuple(1, 2), 'foo'),
    AsTuple(AsTuple(1, 3), 'bar')
);

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
