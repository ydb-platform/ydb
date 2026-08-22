PRAGMA YqlSelect = 'force';

SELECT
    1 IN (
        SELECT
            *
        FROM (
            SELECT
                1 AS a
        ) AS x
    )
FROM (
    VALUES
        (2)
) AS y (
    b
);
