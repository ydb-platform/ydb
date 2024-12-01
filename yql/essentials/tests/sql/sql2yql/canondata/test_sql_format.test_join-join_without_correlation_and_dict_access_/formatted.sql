/* syntax version 1 */
PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

$data_dict = (
    SELECT
        mod,
        YQL::ToIndexDict(ListSort(aggregate_list(key))) AS dk,
        ListSort(aggregate_list(subkey)) AS ls,
        ListSort(aggregate_list(value)) AS lv
    FROM Input
    GROUP BY
        CAST(key AS uint32) % 10 AS mod
);

--INSERT INTO Output
SELECT
    --DISTINCT
    dk[2],
    key,
    value
FROM Input
JOIN $data_dict
    AS d
ON CAST(Input.key AS uint32) / 100 == d.mod
ORDER BY
    key,
    value;
