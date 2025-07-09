PRAGMA DistinctOverKeys;

SELECT
    x,
    count(DISTINCT x) AS cnt
FROM (
    VALUES
        (1),
        (1)
) AS a (
    x
)
GROUP BY
    x
;
