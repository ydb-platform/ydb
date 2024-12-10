/* syntax version 1 */
USE plato;

PRAGMA yt.JoinEnableStarJoin = "true";

$leftSemi =
    SELECT
        *
    FROM
        Input1 AS a
    LEFT SEMI JOIN
        Input2 AS b
    ON
        b.k2 == a.k1 AND a.v1 == b.v2
;

$rightOnly =
    SELECT
        *
    FROM
        Input3 AS c
    RIGHT ONLY JOIN
        $leftSemi AS ls
    ON
        ls.k1 == c.k3 AND ls.v1 == c.v3
;

$left =
    SELECT
        *
    FROM
        $rightOnly AS ro
    LEFT JOIN
        Input4 AS d
    ON
        ro.v1 == d.v4 AND d.k4 == ro.k1
;

$inner =
    SELECT
        *
    FROM ANY
        Input5 AS e
    JOIN
        $left AS l
    ON
        e.k5 == l.k1 AND l.v1 == e.v5
;

SELECT
    *
FROM
    $inner
ORDER BY
    u1,
    u5
;
