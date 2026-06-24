/* postgres can not */
SELECT
    a,
    b,
    c,
    RandomNumber(a) AS r
FROM (
    SELECT
        1 AS a,
        2 AS b,
        3 AS c
    UNION ALL
    SELECT
        1 AS a,
        2 AS b,
        3 AS c
);
