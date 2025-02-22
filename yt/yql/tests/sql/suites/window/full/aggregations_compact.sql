/* syntax version 1 */
/* postgres can not */

SELECT
    value,
    SUM(unwrap(cast(subkey as uint32))) over w1 as sum1,
    COUNT(*)                            over w1 as count1,
    ListSort(AGGREGATE_LIST_DISTINCT(subkey) over w1) as agglist_distinct1,

    SUM(cast(subkey as uint32))         over w2 as sum2,
    AGGREGATE_LIST(subkey)              over w2 as agglist2,
FROM (SELECT * FROM plato.Input WHERE key = '1')
WINDOW
    w1 as (PARTITION COMPACT BY ()),
    w2 as (PARTITION COMPACT BY key ORDER BY value DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
ORDER BY value;
