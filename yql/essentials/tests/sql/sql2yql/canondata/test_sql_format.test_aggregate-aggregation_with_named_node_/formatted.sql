/* syntax version 1 */
/* postgres can not */
$data = (
    SELECT
        CAST(key AS Uint32) ?? 0 AS key,
        value
    FROM plato.Input
);
$quant = 0.1;

SELECT
    $quant * 100 AS quantile,
    PERCENTILE(key, $quant) AS key_q,
    COUNT(*) AS count
FROM $data;
