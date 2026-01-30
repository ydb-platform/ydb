PRAGMA YqlSelect = 'force';

SELECT
    a
FROM (
    VALUES
        (1, 10),
        (2, 20),
        (3, 30)
) AS y (
    a,
    b
)
WHERE
    a == (
        SELECT
            1
    )
ORDER BY
    a
;
