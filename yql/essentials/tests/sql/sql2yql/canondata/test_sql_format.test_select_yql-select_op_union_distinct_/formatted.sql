PRAGMA YqlSelect = 'force';

(
    SELECT
        *
    FROM (
        VALUES
            (1)
    ) AS t (
        x
    )
)
UNION DISTINCT
(
    SELECT
        *
    FROM (
        VALUES
            (1)
    ) AS t (
        x
    )
);
