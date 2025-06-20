SELECT
    *
FROM (
    VALUES
        (1),
        (1),
        (1),
        (2),
        (NULL),
        (NULL),
        (NULL)
) AS t (
    x
)
EXCEPT DISTINCT
SELECT
    *
FROM (
    VALUES
        (1),
        (NULL)
) AS t (
    x
);

SELECT
    *
FROM (
    VALUES
        (1, NULL),
        (1, NULL),
        (1, NULL),
        (NULL, 2),
        (NULL, NULL),
        (NULL, NULL),
        (NULL, NULL)
) AS t (
    x,
    y
)
EXCEPT DISTINCT
SELECT
    *
FROM (
    VALUES
        (1, NULL),
        (NULL, NULL)
) AS t (
    x,
    z
);
