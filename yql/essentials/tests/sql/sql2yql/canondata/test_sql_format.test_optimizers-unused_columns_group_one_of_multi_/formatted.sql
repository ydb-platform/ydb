USE plato;

SELECT
    a,
    cc1
FROM (
    SELECT
        a,
        count(DISTINCT b) AS bb,
        max(c) AS cc,
        median(c) AS cc1,
        percentile(c, 0.8) AS cc2
    FROM (
        SELECT
            a,
            b,
            CAST(c AS int32) AS c,
            d
        FROM Input
    )
    GROUP BY
        a
);
