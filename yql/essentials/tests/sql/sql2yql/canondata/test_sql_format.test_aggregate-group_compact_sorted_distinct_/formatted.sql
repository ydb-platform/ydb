/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    key,
    count(DISTINCT value) AS cnt,
    ListSort(aggregate_list(DISTINCT value)) AS lst,
    min(value) AS min,
    max(value) AS max
FROM
    Input
GROUP COMPACT BY
    key
ORDER BY
    key
;
