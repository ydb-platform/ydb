INSERT INTO plato.Output
SELECT
    *
FROM (
    SELECT
        lead(TableRow()) OVER ()
    FROM
        plato.Input
)
    FLATTEN COLUMNS
;
