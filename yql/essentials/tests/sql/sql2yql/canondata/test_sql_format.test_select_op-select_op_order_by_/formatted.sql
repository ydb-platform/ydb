(
    SELECT
        *
    FROM (
        VALUES
            (1),
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
SELECT
    *
FROM (
    VALUES
        (3)
) AS t (
    x
)
ORDER BY
    x
;

(
    SELECT
        *
    FROM (
        VALUES
            (1),
            (3)
    ) AS t (
        x
    )
    ORDER BY
        x
    LIMIT 4
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
    ORDER BY
        x
    LIMIT 4
)
EXCEPT
SELECT
    *
FROM (
    VALUES
        (3)
) AS t (
    x
)
ORDER BY
    x
;
