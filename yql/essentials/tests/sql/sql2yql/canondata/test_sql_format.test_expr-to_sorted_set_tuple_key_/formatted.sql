/* postgres can not */
$l = AsList(
    AsTuple(AsTuple(), Void()),
    AsTuple(AsTuple(), Void()),
    AsTuple(AsTuple(), Void())
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

$l = AsList(
    AsTuple(AsTuple(1), Void()),
    AsTuple(AsTuple(2), Void()),
    AsTuple(AsTuple(2), Void())
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

$l = AsList(
    AsTuple(AsTuple(1, 2), Void()),
    AsTuple(AsTuple(1, 3), Void()),
    AsTuple(AsTuple(1, 3), Void())
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
