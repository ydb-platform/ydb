/* syntax version 1 */
/* postgres can not */
SELECT
    Some(key) as some_key,
FROM
    plato.Input
GROUP BY
    key
ORDER BY some_key;
