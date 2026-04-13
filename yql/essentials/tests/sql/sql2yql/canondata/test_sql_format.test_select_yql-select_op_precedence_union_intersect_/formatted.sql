PRAGMA YqlSelect = 'force';

(
    SELECT
        *
    FROM (
        VALUES
            (1),
            (2)
    ) AS t (
        x
    )
)
UNION
(
    SELECT
        *
    FROM (
        VALUES
            (2),
            (3)
    ) AS t (
        x
    )
)
INTERSECT
(
    SELECT
        *
    FROM (
        VALUES
            (3)
    ) AS t (
        x
    )
);
