/* ignore yt detailed plan diff */
PRAGMA DisableSimpleColumns;

USE plato;

PRAGMA yt.JoinMergeTablesLimit = '10';
PRAGMA yt.JoinAllowColumnRenames = 'true';

$inner = (
    FROM
        Input1 AS a
    JOIN
        Input2 AS b
    ON
        b.k2 == a.k1 AND a.v1 == b.v2
    SELECT
        b.k2 AS bk2,
        b.k2 AS bk2_extra,
        b.v2 AS bv2,
        a.k1 AS ak1
);

SELECT
    *
FROM
    $inner
ORDER BY
    bv2,
    bk2_extra
;

SELECT
    *
FROM
    $inner
ORDER BY
    bk2,
    bk2_extra,
    bv2
;

SELECT
    *
FROM
    $inner
ORDER BY
    bk2_extra,
    bv2
;

SELECT
    *
FROM
    $inner
ORDER BY
    bk2_extra
;

SELECT
    *
FROM
    $inner
ORDER BY
    ak1
;
