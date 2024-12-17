/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA warning('disable', '4504');

$t = [<|k: 1, v: 2|>];

$src = (
    SELECT
        k
    FROM
        as_table($t)
    ORDER BY
        x
);

SELECT
    *
FROM
    $src
;

$src = (
    SELECT
        a.k AS key
    FROM
        as_table($t) AS a
    JOIN
        as_table($t) AS b
    ON
        a.k == b.k
    ORDER BY
        b.u
);

SELECT
    *
FROM
    $src
;

$src = (
    SELECT
        a.k AS key
    FROM
        as_table($t) AS a
    JOIN
        as_table($t) AS b
    ON
        a.k == b.k
    ORDER BY
        v
);

SELECT
    *
FROM
    $src
;

$src = (
    SELECT
        a.k AS key
    FROM
        as_table($t) AS a
    JOIN
        as_table($t) AS b
    ON
        a.k == b.k
    ORDER BY
        z
);

SELECT
    *
FROM
    $src
;
