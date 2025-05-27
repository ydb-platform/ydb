/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
$src = [
    <|a: 5, b: 50, date: 500|>,
    <|a: 4, b: 40, date: 400|>,
    <|a: 3, b: 30, date: 300|>,
    <|a: 2, b: 20, date: 200|>,
    <|a: 1, b: 10, date: 100|>,
];

$src1 = [
    <|e: 5, f: 50|>,
    <|e: 4, f: 40|>,
    <|e: 3, f: 30|>,
    <|e: 2, f: 20|>,
    <|e: 1, f: 10|>,
];

$src = (
    SELECT
        *
    FROM
        as_table($src)
);

$src1 = (
    SELECT
        *
    FROM
        as_table($src1)
);

SELECT
    a,
    b
FROM
    $src
ORDER BY
    date + 1
;

SELECT
    x.a,
    b
FROM
    $src AS x
ORDER BY
    x.date + 1
;

SELECT
    *
WITHOUT
    b,
    a
FROM
    $src
ORDER BY
    date + 1
;

SELECT
    *
WITHOUT
    b,
    a,
    date
FROM
    $src
ORDER BY
    date + 1
;

SELECT
    *
WITHOUT
    x.b,
    x.a
FROM
    $src AS x
ORDER BY
    date + 1
;

SELECT
    *
WITHOUT
    x.b,
    x.a,
    date
FROM
    $src AS x
ORDER BY
    date + 1
;

SELECT
    a,
    b,
    x.*
WITHOUT
    b,
    a
FROM
    $src AS x
ORDER BY
    date + 1
;

SELECT
    a,
    b,
    x.*
WITHOUT
    b,
    a,
    x.date
FROM
    $src AS x
ORDER BY
    date + 1
;

SELECT
    a,
    b,
    x.*
WITHOUT
    b,
    a,
    x.date
FROM
    $src AS x
ORDER BY
    x.date + 1
;

SELECT
    y.e,
    y.f
FROM
    $src AS x
JOIN
    $src1 AS y
ON
    x.a == y.e
ORDER BY
    x.date
;

SELECT
    *
WITHOUT
    x.a,
    x.b,
FROM
    $src AS x
JOIN
    $src1 AS y
ON
    x.a == y.e
ORDER BY
    date
;

SELECT
    x.*
WITHOUT
    x.date
FROM
    $src AS x
JOIN
    $src1 AS y
ON
    x.a == y.e
ORDER BY
    x.date
;

SELECT
    x.*,
    unwrap(x.date) AS date,
WITHOUT
    x.a,
    x.date
FROM
    $src AS x
ORDER BY
    date
;

SELECT
    x.*,
    unwrap(x.date) AS date,
WITHOUT
    x.a,
    x.date
FROM
    $src AS x
JOIN
    $src1 AS y
ON
    x.a == y.e
ORDER BY
    x.date
;

SELECT
    x.*,
    unwrap(x.date) AS date,
WITHOUT
    x.a,
    x.date
FROM
    $src AS x
JOIN
    $src1 AS y
ON
    x.a == y.e
ORDER BY
    date
;
