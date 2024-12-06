PRAGMA DisableSimpleColumns;

/* postgres can not */
$i = (
    SELECT
        1 AS key
);

$j1 = (
    SELECT
        a2.key,
        b.key AS k
    FROM
        $i AS a2
    JOIN
        $i AS b
    ON
        a2.key == b.key
);

SELECT
    a.*
FROM
    $j1 AS a
JOIN
    $i AS d
ON
    a.k == d.key
;

$j2 = (
    SELECT
        a.key,
        b.key AS k
    FROM
        $i AS a
    JOIN
        $i AS b
    ON
        a.key == b.key
);

SELECT
    a.*
FROM
    $j2 AS a
JOIN
    $i AS d
ON
    a.k == d.key
;
