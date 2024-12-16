/* postgres can not */
/* syntax version 1 */
USE plato;

PRAGMA PositionalUnionAll;
PRAGMA yt.UseNativeYtTypes;

$i = (
    SELECT
        key,
        AGGREGATE_LIST(subkey) AS lst
    FROM
        Input
    GROUP BY
        key
);

INSERT INTO Output
SELECT
    a.key AS key,
    lst ?? [] AS lst,
    2 AS anum,
FROM
    $i AS a
;
