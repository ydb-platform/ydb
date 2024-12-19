INSERT INTO plato.Output
SELECT
    *
FROM (
    SELECT
        IF(key == 'foo', LAG(data) OVER w, data)
    FROM (
        SELECT
            TableRow() AS data,
            key,
            value
        FROM
            plato.Input
    )
    WINDOW
        w AS (
            PARTITION BY
                key
        )
)
    FLATTEN COLUMNS
;
