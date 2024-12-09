/* postgres can not */
USE plato;

$x = (
    SELECT
        *
    FROM Input
    ORDER BY
        value
    LIMIT 10
);

SELECT
    *
FROM $x
WHERE key > "000"
LIMIT coalesce(CAST(0.1 * 0 AS Uint64), 0);
