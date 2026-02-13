PRAGMA YqlSelect = 'force';

SELECT
    a,
    Count(b)
FROM (
    VALUES
        (1, 11),
        (2, 21),
        (2, 22),
        (3, 31),
        (3, 32),
        (3, 33)
) AS x (
    a,
    b
)
GROUP BY
    a
;
