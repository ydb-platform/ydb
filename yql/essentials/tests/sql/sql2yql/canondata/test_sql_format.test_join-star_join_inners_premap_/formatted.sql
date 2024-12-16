/* syntax version 1 */
USE plato;

PRAGMA yt.JoinEnableStarJoin = "true";
PRAGMA DisablePullUpFlatMapOverJoin;

$a =
    SELECT
        k1,
        v1,
        u1,
        1 AS t1
    FROM
        Input1
;

$c =
    SELECT
        k3,
        v3,
        u3,
        3 AS t3
    FROM
        Input3
;

FROM ANY
    Input2 AS b
JOIN ANY
    $a AS a
ON
    b.k2 == a.k1 AND a.v1 == b.v2
JOIN ANY
    $c AS c
ON
    a.k1 == c.k3 AND c.v3 == a.v1
SELECT
    *
ORDER BY
    u1
;
