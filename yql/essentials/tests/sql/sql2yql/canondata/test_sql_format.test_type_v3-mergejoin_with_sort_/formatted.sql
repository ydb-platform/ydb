/* syntax version 1 */
/* postgres can not */
USE plato;
PRAGMA yt.JoinMergeTablesLimit = "100";
PRAGMA yt.JoinMergeForce = "true";

SELECT
    a.key AS key,
    a.subkey AS s1,
    b.subkey AS s2
FROM
    Input1 AS a
JOIN
    Input2 AS b
USING (key)
ORDER BY
    key
;
