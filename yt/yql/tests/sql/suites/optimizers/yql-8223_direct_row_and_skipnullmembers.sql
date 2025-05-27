/* postgres can not */
/* syntax version 1 */
use plato;
pragma AnsiInForEmptyOrNullableItemsCollections;

$x = (
    SELECT
        TableName() AS path,
        key, subkey, value
        FROM CONCAT(Input, Input)
);
$y = (
    SELECT
        t.path AS path,
        t.key as key,
        info.value as value
    FROM $x AS t
    INNER JOIN (
        SELECT key, subkey, value FROM Input
        WHERE key != ""
    ) AS info
    USING (key)
    WHERE t.key in ("023", "150")
);
select distinct path from $y;
