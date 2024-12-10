/* syntax version 1 */
/* postgres can not */
PRAGMA sampleselect;

$data_deep = (
    SELECT
        mod,
        aggregate_list(key) AS lk,
        aggregate_list(subkey) AS ls,
        aggregate_list(value) AS lv
    FROM
        plato.Input
    GROUP BY
        CAST(key AS uint32) % 10 AS mod
);

-- order to have same results on yamr and yt
SELECT
    *
FROM
    $data_deep
    FLATTEN BY (
        lk AS ik,
        ls,
        lv
    )
ORDER BY
    mod,
    ik,
    ls,
    lv
;
