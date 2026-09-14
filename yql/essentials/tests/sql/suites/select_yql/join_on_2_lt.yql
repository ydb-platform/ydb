PRAGMA YqlSelect = 'force';

SELECT
    a,
    b
FROM (
    VALUES
        (1),
        (2),
        (3)
) AS x (
    a
)
JOIN (
    VALUES
        (1),
        (2),
        (3)
) AS y (
    b
)
ON
    x.a < y.b
;
