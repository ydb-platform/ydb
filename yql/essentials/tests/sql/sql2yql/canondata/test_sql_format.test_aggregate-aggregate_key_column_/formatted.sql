/* syntax version 1 */
/* postgres can not */
SELECT
    Some(key) AS some_key,
FROM plato.Input
GROUP BY
    key
ORDER BY
    some_key;
