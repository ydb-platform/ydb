/* syntax version 1 */
/* postgres can not */
$data = (
    SELECT
        mod,
        aggregate_list(value) AS lv
    FROM
        plato.Input
    GROUP BY
        CAST(key AS uint32) % 10 AS mod
);

SELECT
    mod,
    iv
FROM
    $data
    FLATTEN BY lv AS iv
ORDER BY
    mod,
    iv
;
