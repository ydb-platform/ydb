/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    key,count(distinct value) as cnt,
    ListSort(aggregate_list(distinct value)) as lst, 
    min(value) as min, max(value) as max
FROM Input
GROUP COMPACT BY key
ORDER BY key;
