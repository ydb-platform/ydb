/* postgres can not */
USE plato;

$max_key = (
    SELECT
        max(key)
    FROM Input
);

SELECT
    CAST(count(*) AS String) || ' (' || CAST($max_key AS String) || '/24)'
FROM Input
WHERE key == $max_key;
