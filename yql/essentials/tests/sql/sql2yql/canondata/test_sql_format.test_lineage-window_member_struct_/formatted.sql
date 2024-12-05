INSERT INTO plato.Output WITH TRUNCATE
SELECT
    *
FROM (
    SELECT
        lag(data) OVER w
    FROM (
        SELECT
            TableRow() AS data,
            key
        FROM plato.Input
    )
    WINDOW
        w AS (
            PARTITION BY
                key
        )
)
    FLATTEN COLUMNS;
