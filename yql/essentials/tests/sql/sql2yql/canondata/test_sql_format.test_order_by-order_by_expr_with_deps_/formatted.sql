/* postgres can not */
/* syntax version 1 */
USE plato;

$list =
    SELECT
        ListSort(aggregate_list(key))
    FROM Input;

SELECT
    *
FROM Input
ORDER BY
    ListIndexOf($list ?? [], key);
