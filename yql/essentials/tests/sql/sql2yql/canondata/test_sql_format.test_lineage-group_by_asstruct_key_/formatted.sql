INSERT INTO plato.Output
SELECT
    *
FROM (
    SELECT
        x
    FROM (
        SELECT
            <|a: key, b: value|> AS x
        FROM plato.Input
    )
    GROUP BY
        x
)
    FLATTEN COLUMNS;
