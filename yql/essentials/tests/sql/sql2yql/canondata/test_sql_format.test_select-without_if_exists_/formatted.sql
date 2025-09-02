SELECT
    *
WITHOUT IF EXISTS
    x
FROM (
    SELECT
        1 AS x,
        2 AS z
);

SELECT
    *
WITHOUT IF EXISTS
    y
FROM (
    SELECT
        1 AS x,
        2 AS z
);
