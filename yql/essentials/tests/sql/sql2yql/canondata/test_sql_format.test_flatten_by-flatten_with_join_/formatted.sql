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
    d.mod,
    d.lv,
    j.key
FROM $data
    AS d
    FLATTEN BY lv
JOIN plato.Input
    AS j
ON d.mod == CAST(j.key AS uint32) / 10 % 10
ORDER BY
    d.mod,
    d.lv;
