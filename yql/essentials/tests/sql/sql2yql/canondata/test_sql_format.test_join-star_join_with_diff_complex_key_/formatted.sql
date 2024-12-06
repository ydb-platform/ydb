/* syntax version 1 */
USE plato;
PRAGMA yt.JoinEnableStarJoin = "true";

SELECT
    *
FROM Input1
    AS a
JOIN ANY Input2
    AS b
ON a.k1 == b.k2 AND a.v1 == b.v2
JOIN ANY Input3
    AS c
ON a.k1 == c.k3 AND a.v1 == c.v3
LEFT JOIN Input4
    AS d
ON (c.v3, c.u3) == (d.v4, d.u4);
