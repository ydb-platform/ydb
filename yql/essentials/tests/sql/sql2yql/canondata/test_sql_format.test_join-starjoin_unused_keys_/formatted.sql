/* syntax version 1 */
USE plato;

PRAGMA yt.JoinEnableStarJoin = "true";

SELECT
    v3
FROM ANY
    Input1 AS a
JOIN ANY
    Input2 AS b
ON
    (a.k1 == b.k2 AND a.v1 == b.v2)
JOIN ANY
    Input3 AS c
ON
    (a.k1 == c.k3 AND a.v1 == c.v3)
JOIN
    Input4 AS d
ON
    (a.k1 == d.k4)
ORDER BY
    v3
;
