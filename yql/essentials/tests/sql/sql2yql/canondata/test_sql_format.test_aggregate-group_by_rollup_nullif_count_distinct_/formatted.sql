SELECT
    NullIf(Count(DISTINCT a), 0)
FROM (
    SELECT
        '1' AS a,
        '2' AS b,
) AS y
GROUP BY
    ROLLUP (b)
;
