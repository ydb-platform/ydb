PRAGMA DisableSimpleColumns;

/* postgres can not */
$i = (
    SELECT
        AsList('foo') AS x
);

$j = (
    SELECT
        Just(AsList('foo')) AS y
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
;

SELECT
    a.x AS zzz
FROM
    $i AS a
LEFT SEMI JOIN
    $j AS b
ON
    a.x == b.y
;

SELECT
    a.x AS zzz
FROM
    $i AS a
LEFT ONLY JOIN
    $j AS b
ON
    a.x == b.y
;

SELECT
    b.y AS fff
FROM
    $i AS a
RIGHT SEMI JOIN
    $j AS b
ON
    a.x == b.y
;

SELECT
    b.y AS fff
FROM
    $i AS a
RIGHT ONLY JOIN
    $j AS b
ON
    a.x == b.y
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
;
