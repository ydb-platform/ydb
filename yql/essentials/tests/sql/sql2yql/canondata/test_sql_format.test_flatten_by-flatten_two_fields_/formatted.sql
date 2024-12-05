/* syntax version 1 */
/* postgres can not */
$data_deep = (
    SELECT
        mod,
        aggregate_list(key) AS lk,
        aggregate_list(subkey) AS ls,
        aggregate_list(value) AS lv
    FROM plato.Input
    GROUP BY
        CAST(key AS uint32) % 10 AS mod
);

SELECT
    *
FROM $data_deep
    FLATTEN BY (
        lk AS ik,
        lv
    )
ORDER BY
    ik,
    lv,
    mod;
