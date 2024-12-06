/* postgres can not */
USE plato;

$avg = (
    SELECT
        AVG(Length(key))
    FROM Input
);

SELECT
    key
FROM Input
LIMIT CAST($avg AS Uint64) ?? 0;
