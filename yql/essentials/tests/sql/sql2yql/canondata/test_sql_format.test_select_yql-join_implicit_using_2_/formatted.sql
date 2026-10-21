PRAGMA YqlSelect = 'force';

SELECT
    *
FROM (
    VALUES
        (1, 2, 3)
) AS x (
    a,
    b,
    cx
)
JOIN (
    VALUES
        (1, 2, 4)
) AS y (
    a,
    b,
    cy
)
ON
    x.a == y.a AND x.b == y.b
;
