INSERT INTO plato.Output
SELECT
    *
FROM (
    SELECT
        TableRow() AS x
    FROM plato.Input
    UNION ALL
    SELECT
        1 AS y
    FROM plato.Input
)
    FLATTEN COLUMNS;
