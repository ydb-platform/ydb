/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA AnsiInForEmptyOrNullableItemsCollections;

$x = (
    SELECT
        TableName() AS path,
        key,
        subkey,
        value
    FROM CONCAT(Input, Input)
);

$y = (
    SELECT
        t.path AS path,
        t.key AS key,
        info.value AS value
    FROM $x
        AS t
    INNER JOIN (
        SELECT
            key,
            subkey,
            value
        FROM Input
        WHERE key != ""
    )
        AS info
    USING (key)
    WHERE t.key IN ("023", "150")
);

SELECT DISTINCT
    path
FROM $y;
