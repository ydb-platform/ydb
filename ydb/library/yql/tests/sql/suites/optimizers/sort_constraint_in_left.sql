/* postgres can not */

USE plato;

$input_table = (SELECT * FROM Input);

SELECT * FROM (
    SELECT 'total' AS key,
        COUNT(*) AS count
    FROM $input_table
    WHERE key != "1"
    UNION ALL
    SELECT key,
        COUNT(*) AS count
    FROM $input_table
    WHERE key != "1"
    GROUP BY key
)
ORDER BY key, count;
