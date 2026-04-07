PRAGMA YqlSelect = 'force';

(
    SELECT
        *
    FROM (
        VALUES
            (1),
            (1),
            (1),
            (1),
            (1)
    ) AS t (
        x
    )
)
EXCEPT ALL
(
    SELECT
        *
    FROM (
        VALUES
            (1),
            (1)
    ) AS t (
        x
    )
);
