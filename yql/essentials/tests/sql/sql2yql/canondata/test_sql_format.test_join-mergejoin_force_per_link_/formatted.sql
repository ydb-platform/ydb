/* syntax version 1 */
/* postgres can not */
USE plato;

PRAGMA yt.JoinMergeTablesLimit = "10";

SELECT
    a.key AS k1,
    b.key AS k2,
    c.key AS k3,
    a.subkey AS sk1,
    b.subkey AS sk2,
    c.subkey AS sk3
FROM
    Input1 AS a
JOIN
    Input2 AS b
ON
    a.key == b.key
JOIN
    /*+ merge() */ Input3 AS c
ON
    b.key == c.key
ORDER BY
    k3
;
