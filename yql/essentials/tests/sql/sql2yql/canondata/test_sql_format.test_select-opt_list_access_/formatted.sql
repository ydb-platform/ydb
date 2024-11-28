/* syntax version 1 */
/* postgres can not */
USE plato;

$data = (
    SELECT
        key,
        Just(aggregate_list(CAST(subkey AS uint32))) AS lsk
    FROM plato.Input
    GROUP BY
        CAST(key AS uint32) AS key
);

SELECT
    key,
    lsk[0]
FROM $data
WHERE lsk IS NOT NULL
ORDER BY
    key;
