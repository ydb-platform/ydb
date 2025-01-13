/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
$src = [
    <|a: 4, b: 4, date: 4|>,
    <|a: 3, b: 3, date: 3|>,
    <|a: 2, b: 2, date: 2|>,
    <|a: 1, b: 1, date: 1|>,
];

SELECT
    a
FROM
    as_table($src)
ORDER BY
    date
;

SELECT
    x.a
FROM
    as_table($src) AS x
ORDER BY
    date
;

SELECT
    x.a
FROM
    as_table($src) AS x
ORDER BY
    x.date
;
