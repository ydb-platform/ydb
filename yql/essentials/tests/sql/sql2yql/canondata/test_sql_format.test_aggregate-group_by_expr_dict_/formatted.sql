/* syntax version 1 */
/* postgres can not */
$data_dict = (
    SELECT
        mod,
        Just(YQL::ToIndexDict(ListSort(aggregate_list(key)))) AS dk,
        ListSort(aggregate_list(subkey)) AS ls,
        ListSort(aggregate_list(value)) AS lv
    FROM
        plato.Input
    GROUP BY
        CAST(subkey AS uint32) % 10 AS mod
);

SELECT
    *
FROM
    $data_dict AS t
GROUP BY
    t.dk[0] AS gk
ORDER BY
    gk
;
