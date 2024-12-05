/* syntax version 1 */
/* postgres can not */
$data_dict = (
    SELECT
        mod,
        Just(ToDict(ListEnumerate(ListTake(ListSort(aggregate_list(key)), 1)))) AS dk,
        ListTake(ListSort(aggregate_list(value)), 1) AS lv
    FROM plato.Input
    GROUP BY
        CAST(subkey AS uint32) % 10 AS mod
);

SELECT
    *
FROM $data_dict
    FLATTEN DICT BY dk
ORDER BY
    mod;
