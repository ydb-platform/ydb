PRAGMA YqlSelect = 'force';

SELECT
    a
FROM (
    VALUES
        (1, 10),
        (2, 20),
        (3, 30)
) AS x (
    a,
    b
)
ORDER BY
    (
        SELECT
            a
    )
;
