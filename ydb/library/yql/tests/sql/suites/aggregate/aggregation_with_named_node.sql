/* syntax version 1 */
/* postgres can not */
$data = (SELECT cast(key as Uint32) ?? 0 as key, value FROM plato.Input);

$quant = 0.1;
SELECT
    $quant * 100 as quantile,
    PERCENTILE(key, $quant) as key_q,
    COUNT(*) as count
FROM $data;
