PRAGMA YqlSelect = 'force';
PRAGMA YqlSelectAllowUnnamedGroupByExpr;

SELECT
    a + b,
    c,
    Count(z)
FROM (
    VALUES
        (1, 11, 1, 11),
        (2, 21, 2, 21),
        (2, 22, 2, 22),
        (3, 31, 3, 31),
        (3, 32, 3, 32),
        (3, 33, 3, 33)
) AS x (
    a,
    b,
    c,
    z
)
GROUP BY
    GROUPING SETS (
        (a + b),
        (c)
    )
;
