/* postgres can not */
USE plato;

$input = (
    SELECT
        *
    FROM (
        SELECT
            Unwrap(some(row))
        FROM (
            SELECT
                TableRow() AS row
            FROM Input
        )
    )
        FLATTEN COLUMNS
);

--INSERT INTO Output WITH TRUNCATE
SELECT
    *
FROM $input;
