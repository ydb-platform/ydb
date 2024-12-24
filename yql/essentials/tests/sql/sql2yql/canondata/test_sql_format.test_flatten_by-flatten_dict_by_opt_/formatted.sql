/* syntax version 1 */
/* postgres can not */
$data_dict = (
    SELECT
        mod,
        YQL::ToIndexDict(ListTake(ListSort(aggregate_list(Just(key))), 1)) AS dk,
        ListTake(ListSort(aggregate_list(subkey)), 1) AS ls
    FROM
        plato.Input
    GROUP BY
        CAST(key AS uint32) % 10 AS mod
);

SELECT
    *
FROM
    $data_dict
    FLATTEN BY (
        dk AS di,
        ls,
        mod
    )
ORDER BY
    mod
;
