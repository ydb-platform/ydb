/* syntax version 1 */
USE plato;

PRAGMA yt.LookupJoinLimit = '64k';
PRAGMA yt.LookupJoinMaxRows = '100';

SELECT
    v3
FROM
    Input1 AS a
JOIN
    Input2 AS b
ON
    (a.k1 == b.k2)
JOIN
    Input3 AS c
ON
    (a.k1 == c.k3)
ORDER BY
    v3
;
