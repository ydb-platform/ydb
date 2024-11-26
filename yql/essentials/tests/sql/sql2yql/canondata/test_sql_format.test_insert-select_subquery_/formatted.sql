/* postgres can not */
USE plato;

$a = (
    SELECT
        key
    FROM Input
    ORDER BY
        key
    LIMIT 1
);

INSERT INTO Output (
    key
)
SELECT
    $a;
