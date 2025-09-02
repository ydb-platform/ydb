/* postgres can not */
USE plato;

SELECT
    *
FROM (SELECT Just(1) AS x)
FLATTEN OPTIONAL BY x;

$lst = AsList(1,2,3);
SELECT
    *
FROM (SELECT $lst AS x)
FLATTEN LIST BY x ORDER BY x;

SELECT
    x
FROM (SELECT Just($lst) AS x)
FLATTEN LIST BY x ORDER BY x;

SELECT
    *
FROM (SELECT Just($lst) AS x)
FLATTEN OPTIONAL BY x ORDER BY x;

$dct = AsDict(AsTuple(1,"foo"),AsTuple(2,"bar"),AsTuple(3,"baz"));

SELECT
    *
FROM (SELECT $dct AS x)
FLATTEN DICT BY x ORDER BY x;

SELECT
    x
FROM (SELECT Just($dct) AS x)
FLATTEN DICT BY x ORDER BY x;

SELECT
    ListSort(DictItems(x))
FROM (SELECT Just($dct) AS x)
FLATTEN OPTIONAL BY x;
