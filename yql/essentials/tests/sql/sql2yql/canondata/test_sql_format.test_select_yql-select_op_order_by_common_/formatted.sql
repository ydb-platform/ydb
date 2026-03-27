PRAGMA YqlSelect = 'force';

(
    SELECT
        *
    FROM (
        VALUES
            (4),
            (3),
            (2),
            (5)
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
            (6),
            (4),
            (3),
            (2),
            (5)
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
