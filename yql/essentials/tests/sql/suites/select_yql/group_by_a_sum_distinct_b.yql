PRAGMA YqlSelect = 'force';

SELECT
    a,
    Sum(DISTINCT b)
FROM (
    VALUES
        (1, 10),
        (1, 11),
        (2, 20),
        (2, 20),
        (2, 21),
        (2, 21),
        (3, 30),
        (3, 30),
        (3, 30),
        (3, 31),
        (3, 31),
        (3, 31)
) AS x (
    a,
    b
)
GROUP BY
    a
;
