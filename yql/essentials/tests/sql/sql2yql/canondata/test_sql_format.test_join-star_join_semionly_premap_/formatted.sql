/* syntax version 1 */
USE plato;

PRAGMA yt.JoinEnableStarJoin = 'true';
PRAGMA DisablePullUpFlatMapOverJoin;

$a = (
    SELECT
        k1,
        v1,
        u1,
        1 AS t1
    FROM
        Input1
);

$b = (
    SELECT
        k2,
        v2,
        u2,
        2 AS t2
    FROM
        Input2
);

$c = (
    SELECT
        k3,
        v3,
        u3,
        3 AS t3
    FROM
        Input3
);

FROM
    $a AS a
LEFT SEMI JOIN
    $b AS b
ON
    a.k1 == b.k2
LEFT ONLY JOIN
    $c AS c
ON
    a.k1 == c.k3
SELECT
    *
ORDER BY
    u1
;
