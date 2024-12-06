/* syntax version 1 */
/* postgres can not */
USE plato;

INSERT INTO @foo
SELECT
    AsList(1, 2) AS x,
    1 AS y
UNION ALL
SELECT
    AsList(1, 3) AS x,
    2 AS y
UNION ALL
SELECT
    AsList(1, 2) AS x,
    3 AS y
;

COMMIT;

SELECT
    x,
    count(*) AS c
FROM
    @foo
GROUP BY
    x
ORDER BY
    c
;

INSERT INTO @bar
SELECT
    AsList(1, 2) AS x,
    AsList(4) AS y,
    1 AS z
UNION ALL
SELECT
    AsList(1, 3) AS x,
    AsList(4) AS y,
    2 AS z
UNION ALL
SELECT
    AsList(1, 3) AS x,
    AsList(4) AS y,
    3 AS z
UNION ALL
SELECT
    AsList(1, 3) AS x,
    AsList(4) AS y,
    4 AS z
UNION ALL
SELECT
    AsList(1, 2) AS x,
    AsList(5) AS y,
    5 AS z
UNION ALL
SELECT
    AsList(1, 2) AS x,
    AsList(5) AS y,
    6 AS z
;

COMMIT;

SELECT
    x,
    y,
    count(*) AS c
FROM
    @bar
GROUP BY
    x,
    y
ORDER BY
    c
;

SELECT
    x,
    y,
    count(DISTINCT z) AS c
FROM
    @bar
GROUP BY
    x,
    y
ORDER BY
    c
;

SELECT
    x,
    y,
    min(z) AS m,
    count(DISTINCT z) AS c
FROM
    @bar
GROUP BY
    x,
    y
ORDER BY
    c
;

SELECT
    x
FROM
    @bar AS t
GROUP BY
    x
ORDER BY
    t.x[1]
;

SELECT
    x,
    y
FROM
    @bar AS t
GROUP BY
    x,
    y
ORDER BY
    t.x[1],
    t.y[0]
;

SELECT DISTINCT
    x
FROM
    @bar AS t
ORDER BY
    t.x[1] DESC
;

SELECT DISTINCT
    x,
    y
FROM
    @bar AS t
ORDER BY
    t.x[1] DESC,
    t.y[0] DESC
;
