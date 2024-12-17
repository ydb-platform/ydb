/* syntax version 1 */
/* postgres can not */
SELECT
    x,
    count(*) AS c
FROM (
    SELECT
        AsList(1, 2) AS x
    UNION ALL
    SELECT
        AsList(1, 3) AS x
    UNION ALL
    SELECT
        AsList(1, 2) AS x
)
GROUP BY
    x
ORDER BY
    c
;

SELECT
    x,
    y,
    count(*) AS c
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
    x,
    y
ORDER BY
    c
;
