/* postgres can not */
$vt = ParseType("Variant<Int32,Uint32>");
$v1 = VARIANT (1, "0", $vt);
$v2 = VARIANT (2u, "1", $vt);
$v3 = VARIANT (2, "0", $vt);

$l = AsList(
    AsTuple($v1, "foo"),
    AsTuple($v2, "bar"),
);

$d = ToSortedDict($l);

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

$d = ToSortedMultiDict($l);

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
