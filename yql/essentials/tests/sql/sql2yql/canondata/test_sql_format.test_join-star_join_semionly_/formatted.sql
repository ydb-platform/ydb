/* syntax version 1 */
USE plato;
PRAGMA yt.JoinEnableStarJoin = "true";

FROM Input1
    AS a
LEFT SEMI JOIN Input2
    AS b
ON a.k1 == b.k2
LEFT ONLY JOIN Input3
    AS c
ON a.k1 == c.k3
SELECT
    *
ORDER BY
    u1;
