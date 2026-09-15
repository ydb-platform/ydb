PRAGMA YqlSelect = 'force';

SELECT
    EXISTS (
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
