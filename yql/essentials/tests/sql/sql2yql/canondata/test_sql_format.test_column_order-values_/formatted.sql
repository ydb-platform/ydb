/* postgres can not */
/* syntax version 1 */
PRAGMA OrderedColumns;

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
    b,
    c
);

SELECT
    *
FROM (
    VALUES
        (1, 2, 3, 4),
        (5, 6, 7, 8)
) AS t (
    b,
    c,
    a
);
