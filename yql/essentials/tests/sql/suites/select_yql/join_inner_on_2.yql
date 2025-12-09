PRAGMA YqlSelect = 'force';

SELECT
    x.a,
    y.b,
FROM (
    VALUES
        (1),
        (2),
        (3)
) AS x (
    a
)
INNER JOIN (
    VALUES
        (1),
        (2),
        (4)
) AS y (
    b
)
ON
    x.a == y.b
;
