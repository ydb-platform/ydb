/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
$src = [
    <|a: 5, b: 50, zz: 500|>,
    <|a: 4, b: 40, zz: 400|>,
    <|a: 3, b: 30, zz: 300|>,
    <|a: 2, b: 20, zz: 200|>,
    <|a: 1, b: 10, zz: 100|>,
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
    x.zz,
    x.b + y.f AS col1
FROM
    $src AS x
CROSS JOIN
    $src1 AS y
WHERE
    x.a == y.e
ORDER BY
    zz,
    col1
;
