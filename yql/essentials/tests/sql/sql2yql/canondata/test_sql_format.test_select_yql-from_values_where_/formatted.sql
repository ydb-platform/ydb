PRAGMA YqlSelect = 'force';

SELECT
    a,
FROM (
    VALUES
        (0),
        (1),
        (2),
        (3),
        (4),
        (5),
        (6)
) AS x (
    a
)
WHERE
    2 <= a
    AND a <= 4
;
