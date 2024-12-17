/* syntax version 1 */
/* postgres can not */
USE plato;

INSERT INTO @foo
SELECT
    AsList(1, 2) AS x
UNION ALL
SELECT
    AsList(1, 3) AS x
UNION ALL
SELECT
    AsList(1, 2) AS x
;

COMMIT;

SELECT
    listlength(aggregate_list(DISTINCT x)) AS c
FROM
    @foo
;

SELECT
    count(DISTINCT x) AS c
FROM
    @foo
;

INSERT INTO @bar
SELECT
    AsList(1, 2) AS x,
    AsList(4) AS y
UNION ALL
SELECT
    AsList(1, 3) AS x,
    AsList(4) AS y
UNION ALL
SELECT
    AsList(1, 3) AS x,
    AsList(4) AS y
UNION ALL
SELECT
    AsList(1, 3) AS x,
    AsList(4) AS y
UNION ALL
SELECT
    AsList(1, 2) AS x,
    AsList(5) AS y
UNION ALL
SELECT
    AsList(1, 2) AS x,
    AsList(5) AS y
;

COMMIT;

SELECT
    x,
    count(DISTINCT y) AS c
FROM
    @bar
GROUP BY
    x
ORDER BY
    c
;
