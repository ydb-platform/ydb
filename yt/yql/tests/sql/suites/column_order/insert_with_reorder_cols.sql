/* postgres can not */
/* syntax version 1 */
USE plato;

PRAGMA PositionalUnionAll;
PRAGMA yt.UseNativeYtTypes;

$i =
SELECT
    key,
    AGGREGATE_LIST(subkey) as lst
FROM Input 
GROUP BY key;

INSERT INTO Output
SELECT
    a.key as key,
    lst ?? [] as lst,
    2 as anum,
FROM $i as a;