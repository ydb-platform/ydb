/* syntax version 1 */
/* postgres can not */
PRAGMA sampleselect;

$data_dict = (
    SELECT
        mod,
        YQL::ToIndexDict(ListSort(aggregate_list(key))) AS dk,
        ListSort(aggregate_list(subkey)) AS ls,
        ListSort(aggregate_list(value)) AS lv
    FROM
        plato.Input
    GROUP BY
        CAST(key AS uint32) % 10 AS mod
);

SELECT
    mod,
    iv,
    ls,
    dd.di.0 AS key,
    dd.di.1 AS value
FROM
    $data_dict AS dd
    FLATTEN BY (
        dk AS di,
        lv AS iv,
        ls
    )
ORDER BY
    mod,
    iv,
    ls,
    key,
    value
;
