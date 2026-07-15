PRAGMA YqlSelect = 'force';

SELECT
    a,
    b,
    Count(z)
FROM (
    VALUES
        (1, 11, 11),
        (2, 21, 21),
        (2, 22, 22),
        (3, 31, 31),
        (3, 32, 32),
        (3, 33, 33)
) AS x (
    a,
    b,
    z
)
GROUP BY
    a,
    CUBE (a, b)
;
