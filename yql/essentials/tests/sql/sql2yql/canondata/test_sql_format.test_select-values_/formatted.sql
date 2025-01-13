/* syntax version 1 */
VALUES
    (1, 2),
    (3, 4)
;

SELECT
    *
FROM (
    VALUES
        (1, 2),
        (3, 4)
);

SELECT
    *
FROM (
    VALUES
        (1, 2),
        (3, 4)
) AS t (
    x,
    y
);

SELECT
    *
FROM (
    VALUES
        (1, 2, 3, 4),
        (5, 6, 7, 8)
) AS t (
    x,
    y,
    z
);

SELECT
    EXISTS (
        VALUES
            (1)
    )
;
