PRAGMA YqlSelect = 'force';

(
    SELECT
        *
    FROM (
        VALUES
            (3)
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
            (2)
    ) AS t (
        x
    )
)
EXCEPT
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
