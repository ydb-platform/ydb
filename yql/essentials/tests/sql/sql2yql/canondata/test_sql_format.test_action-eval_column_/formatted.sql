/* syntax version 1 */
/* postgres can not */
USE plato;
$x = CAST(Unicode::ToLower("foo"u) AS String);

SELECT
    AsStruct("1" AS foo, 2 AS bar).$x
;
$x = CAST(Unicode::ToLower("value"u) AS String);

SELECT
    key,
    t.$x
FROM
    Input AS t
ORDER BY
    key
;
$x = CAST(Unicode::ToLower("value"u) AS String);

SELECT
    key,
    TableRow().$x
FROM
    Input
ORDER BY
    key
;
$x = CAST(Unicode::ToLower("value"u) AS String);

SELECT
    *
FROM
    Input AS t
ORDER BY
    t.$x
;
$x = CAST(Unicode::ToLower("value"u) AS String);
$y = CAST(Unicode::ToLower("key"u) AS String);

SELECT
    x,
    count(*)
FROM
    Input AS t
GROUP BY
    t.$x AS x
HAVING
    min(t.$y) != ""
ORDER BY
    x
;

SELECT
    a.$x AS x,
    b.$y AS y
FROM
    Input AS a
JOIN
    Input AS b
ON
    (a.$x == b.$x)
ORDER BY
    x
;

SELECT
    a.$x AS x,
    b.$y AS y
FROM
    Input AS a
JOIN
    Input AS b
USING ($x)
ORDER BY
    x
;

SELECT
    p,
    value,
    lag(value) OVER w AS lag
FROM
    Input
WINDOW
    w AS (
        PARTITION BY
            TableRow().$y AS p
        ORDER BY
            TableRow().$x
    )
ORDER BY
    p,
    value
;
