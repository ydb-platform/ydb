/* syntax version 1 */
/* postgres can not */
SELECT
    listlength(aggregate_list(DISTINCT x)) AS c
FROM (
    SELECT
        AsList(1, 2) AS x
    UNION ALL
    SELECT
        AsList(1, 3) AS x
    UNION ALL
    SELECT
        AsList(1, 2) AS x
);

SELECT
    count(DISTINCT x) AS c
FROM (
    SELECT
        AsList(1, 2) AS x
    UNION ALL
    SELECT
        AsList(1, 3) AS x
    UNION ALL
    SELECT
        AsList(1, 2) AS x
);

SELECT
    x,
    count(DISTINCT y) AS c
FROM (
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
)
GROUP BY
    x
ORDER BY
    c
;
