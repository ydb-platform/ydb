(
    SELECT
        *
    FROM (
        VALUES
            (1),
            (2),
            (3),
            (4)
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
            (0),
            (1)
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
            (2),
            (5),
            (6)
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
            (4)
    ) AS t (
        x
    )
);
