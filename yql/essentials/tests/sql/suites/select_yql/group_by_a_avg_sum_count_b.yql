PRAGMA YqlSelect = 'force';

SELECT
    a,
    Avg(b) AS avg1,
    Sum(b) / Count(b) AS avg2
FROM (
    VALUES
        (1, 100),
        (2, 200),
        (2, 250),
        (3, 300),
        (3, 300),
        (3, 390)
) AS x (
    a,
    b
)
GROUP BY
    a
;
