INSERT INTO plato.Output
SELECT
    *
FROM (
    SELECT
        lead(<|a: key, b: value|>) OVER ()
    FROM plato.Input
)
    FLATTEN COLUMNS;
