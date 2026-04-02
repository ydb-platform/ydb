PRAGMA YqlSelect = 'force';
PRAGMA YqlSelectAllowUnnamedGroupByExpr;

SELECT
    a + b,
    Sum(z)
FROM (
    VALUES
        (1, 1, 1),
        (2, 2, 2),
        (3, 3, 3)
) AS x (
    a,
    b,
    z
)
GROUP BY
    a + b
;
