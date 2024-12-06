/* syntax version 1 */
USE plato;
PRAGMA yt.JoinEnableStarJoin = "true";

FROM ANY
    Input2 AS b
JOIN ANY
    Input1 AS a
ON
    b.k2 == a.k1 AND a.v1 == b.v2
JOIN ANY
    Input3 AS c
ON
    a.k1 == c.k3 AND c.v3 == a.v1
SELECT
    *
ORDER BY
    u1
;
