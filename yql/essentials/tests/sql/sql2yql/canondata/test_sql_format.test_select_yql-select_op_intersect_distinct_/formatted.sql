PRAGMA YqlSelect = 'force';

(
    SELECT
        *
    FROM (
        VALUES
            (1),
            (2),
            (2),
            (2),
            (3)
    ) AS t (
        x
    )
)
INTERSECT ALL
(
    SELECT
        *
    FROM (
        VALUES
            (1),
            (2),
            (2)
    ) AS t (
        x
    )
);
