/* syntax version 1 */
USE plato;
PRAGMA yt.JoinEnableStarJoin = "true";

$rightSemi =
    SELECT
        *
    FROM
        Input2 AS b
    RIGHT SEMI JOIN
        Input1 AS a
    ON
        a.v1 == b.v2 AND a.k1 == b.k2
;

$leftOnly =
    SELECT
        *
    FROM
        $rightSemi AS rs
    LEFT ONLY JOIN
        Input3 AS c
    ON
        rs.k1 == c.k3 AND rs.v1 == c.v3
;

$right =
    SELECT
        *
    FROM
        Input4 AS d
    RIGHT JOIN
        $leftOnly AS lo
    ON
        d.v4 == lo.v1 AND lo.k1 == d.k4
;

$inner =
    SELECT
        *
    FROM
        $right AS r
    JOIN ANY
        Input5 AS e
    ON
        r.k1 == e.k5 AND e.v5 == r.v1
;

SELECT
    *
FROM
    $inner
ORDER BY
    u1,
    u5
;
