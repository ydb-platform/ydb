PRAGMA YqlSelect = 'force';

SELECT
    *
FROM (
    VALUES
        (1, 2)
) AS x (
    a,
    cx
)
RIGHT JOIN (
    VALUES
        (1, 3),
        (2, 4)
) AS y (
    a,
    cy
)
ON
    x.a == y.a
;
