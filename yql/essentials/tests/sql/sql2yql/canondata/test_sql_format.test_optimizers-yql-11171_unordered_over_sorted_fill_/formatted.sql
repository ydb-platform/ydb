/* syntax version 1 */
USE plato;

SELECT
    key,
    some(value)
FROM (
    SELECT
        key,
        TableName() AS value
    FROM Input
        WITH inline
    UNION ALL
    SELECT
        key,
        value
    FROM Input
)
GROUP COMPACT BY
    key;
