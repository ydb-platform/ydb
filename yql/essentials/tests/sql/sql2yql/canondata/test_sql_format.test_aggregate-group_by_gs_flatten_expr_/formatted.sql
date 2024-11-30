/* syntax version 1 */
/* postgres can not */
USE plato;

$input =
    SELECT
        a.*,
        [1, 2] AS lst
    FROM Input
        AS a;

SELECT
    key,
    subkey,
    count(lst) AS lst_count
FROM $input
    FLATTEN LIST BY (
        ListExtend(lst, [3, 4]) AS lst
    )
WHERE lst != 2
GROUP BY
    GROUPING SETS (
        (key),
        (key, subkey))
ORDER BY
    key,
    subkey;
