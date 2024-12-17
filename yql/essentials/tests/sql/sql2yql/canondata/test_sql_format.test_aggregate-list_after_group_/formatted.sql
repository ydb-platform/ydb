/* syntax version 1 */
/* postgres can not */
SELECT
    aggregate_list(key) AS key_list,
    listsort(aggregate_list(key)) AS sorted_key_list,
    value AS name
FROM
    plato.Input4
GROUP BY
    value
ORDER BY
    name
;
