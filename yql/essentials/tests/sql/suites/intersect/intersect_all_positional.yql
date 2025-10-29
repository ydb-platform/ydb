PRAGMA PositionalUnionAll;

SELECT
    *
FROM (
    VALUES
        (1),
        (1),
        (2),
        (NULL),
        (NULL)
) AS t (
    x
)
INTERSECT ALL
SELECT
    *
FROM (
    VALUES
        (1),
        (1),
        (NULL),
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
        (NULL, 2),
        (NULL, NULL),
        (NULL, NULL)
) AS t (
    x,
    y
)
INTERSECT ALL
SELECT
    *
FROM (
    VALUES
        (1, NULL),
        (1, NULL),
        (NULL, NULL),
        (NULL, NULL)
) AS t (
    w,
    z
);

SELECT
    *
FROM (
    VALUES
        (1, NULL),
        (1, NULL),
        (NULL, 2),
        (NULL, NULL),
        (NULL, NULL)
) AS t (
    x,
    y
)
INTERSECT ALL
SELECT
    *
FROM (
    VALUES
        (1, NULL),
        (1, NULL),
        (NULL, NULL),
        (NULL, NULL)
) AS t (
    y,
    x
);
