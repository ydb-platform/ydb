/* postgres can not */
USE plato;

$input = (SELECT *
    FROM (
        SELECT Unwrap(some(row))
        FROM (
            SELECT TableRow() as row
            FROM Input
        )
    ) FLATTEN COLUMNS
);

--INSERT INTO Output WITH TRUNCATE
SELECT *
FROM $input
