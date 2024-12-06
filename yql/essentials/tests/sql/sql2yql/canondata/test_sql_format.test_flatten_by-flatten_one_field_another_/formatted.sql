/* syntax version 1 */
/* postgres can not */
$data = (
    SELECT
        mod,
        aggregate_list(value) AS lv
    FROM plato.Input
    GROUP BY
        CAST(key AS uint32) % 10 AS mod
);

SELECT
    d.lv,
    d.mod
FROM $data
    AS d
    FLATTEN BY (
        lv
    )
ORDER BY
    lv;
