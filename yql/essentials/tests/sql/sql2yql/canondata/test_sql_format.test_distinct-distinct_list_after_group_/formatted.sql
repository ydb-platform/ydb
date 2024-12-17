/* syntax version 1 */
/* postgres can not */
SELECT
    listsort(aggregate_list(DISTINCT key)) AS key_list,
    value AS name
FROM
    plato.Input3
GROUP BY
    value
ORDER BY
    name
;
