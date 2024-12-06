PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

$i = (
    SELECT
        AsList(key) AS x
    FROM
        Input
);

$j = (
    SELECT
        Just(AsList(key)) AS y
    FROM
        Input
);

SELECT
    a.x AS zzz,
    b.y AS fff
FROM
    $i AS a
INNER JOIN
    $j AS b
ON
    a.x == b.y
ORDER BY
    zzz,
    fff
;

SELECT
    a.x AS zzz,
    b.y AS fff
FROM
    $i AS a
RIGHT JOIN
    $j AS b
ON
    a.x == b.y
ORDER BY
    zzz,
    fff
;

SELECT
    a.x AS zzz,
    b.y AS fff
FROM
    $i AS a
LEFT JOIN
    $j AS b
ON
    a.x == b.y
ORDER BY
    zzz,
    fff
;

SELECT
    a.x AS zzz
FROM
    $i AS a
LEFT SEMI JOIN
    $j AS b
ON
    a.x == b.y
ORDER BY
    zzz
;

SELECT
    a.x AS zzz
FROM
    $i AS a
LEFT ONLY JOIN
    $j AS b
ON
    a.x == b.y
ORDER BY
    zzz
;

SELECT
    b.y AS fff
FROM
    $i AS a
RIGHT SEMI JOIN
    $j AS b
ON
    a.x == b.y
ORDER BY
    fff
;

SELECT
    b.y AS fff
FROM
    $i AS a
RIGHT ONLY JOIN
    $j AS b
ON
    a.x == b.y
ORDER BY
    fff
;

SELECT
    a.x AS zzz,
    b.y AS fff
FROM
    $i AS a
FULL JOIN
    $j AS b
ON
    a.x == b.y
ORDER BY
    zzz,
    fff
;

SELECT
    a.x AS zzz,
    b.y AS fff
FROM
    $i AS a
EXCLUSION JOIN
    $j AS b
ON
    a.x == b.y
ORDER BY
    zzz,
    fff
;
