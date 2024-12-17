/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    key,count(distinct value) as cnt,
    ListSort(ListMap(
        aggregate_list(distinct value),($x)->{ return DictItems($x) })) as lst
FROM (SELECT key, AsDict(AsTuple(1, value)) as value from Input)
GROUP COMPACT BY key
ORDER BY key;
