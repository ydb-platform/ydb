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
LEFT OUTER JOIN (
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
