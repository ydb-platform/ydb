/* custom error: Unable to use duplicate column names. Collision in name: c */
PRAGMA YqlSelect = 'force';

SELECT
    *
FROM (
    VALUES
        (1, 2)
) AS x (
    a,
    c
)
JOIN (
    VALUES
        (1, 3)
) AS y (
    a,
    c
)
ON
    x.a == y.a
;
