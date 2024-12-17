INSERT INTO plato.Output
SELECT
    *
FROM (
    SELECT
        x,
        y
    FROM (
        SELECT
            key,
            <|a: key, b: value|> AS x
        FROM
            plato.Input
    ) AS a
    JOIN (
        SELECT
            key,
            <|c: key, d: value|> AS y
        FROM
            plato.Input
    ) AS b
    ON
        a.key == b.key
)
    FLATTEN COLUMNS
;
