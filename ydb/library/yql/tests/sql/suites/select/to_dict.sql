/* postgres can not */
/* syntax version 1 */
SELECT
    key,
    ToDict(items) AS `dict`,
    ToMultiDict(items) AS multi_dict
FROM (
    SELECT
        key,
        AGGREGATE_LIST(AsTuple(subkey, value)) AS items
    FROM (
        SELECT * FROM plato.Input
        UNION ALL
        SELECT * FROM plato.Input
    ) GROUP BY key
)
ORDER BY key;
