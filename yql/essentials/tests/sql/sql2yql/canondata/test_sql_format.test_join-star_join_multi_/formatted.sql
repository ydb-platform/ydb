/* syntax version 1 */
USE plato;
PRAGMA yt.JoinEnableStarJoin = "true";

-- first Star JOIN chain 
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

$chain1 =
    SELECT
        *
    FROM
        $right AS r
    JOIN ANY
        Input5 AS e
    ON
        r.k1 == e.k5 AND e.v5 == r.v1
;

-- second Star JOIN chain (mirror reflection of first one)
$leftSemi =
    SELECT
        *
    FROM
        Input1 AS a1
    LEFT SEMI JOIN
        Input2 AS b1
    ON
        b1.k2 == a1.k1 AND a1.v1 == b1.v2
;

$rightOnly =
    SELECT
        *
    FROM
        Input3 AS c1
    RIGHT ONLY JOIN
        $leftSemi AS ls
    ON
        ls.k1 == c1.k3 AND ls.v1 == c1.v3
;

$left =
    SELECT
        *
    FROM
        $rightOnly AS ro
    LEFT JOIN
        Input4 AS d1
    ON
        ro.v1 == d1.v4 AND d1.k4 == ro.k1
;

$chain2 =
    SELECT
        *
    FROM ANY
        Input5 AS e1
    JOIN
        $left AS l
    ON
        e1.k5 == l.k1 AND l.v1 == e1.v5
;

SELECT
    left.k1 AS k1,
    right.v1 AS v1
FROM
    $chain1 AS left
JOIN
    $chain2 AS right
ON
    left.k1 == right.k1 AND left.v1 == right.v1
ORDER BY
    k1,
    v1
;
