/* syntax version 1 */
/* postgres can not */

PRAGMA warning("disable", "4520");

SELECT
    value,
    SUM(unwrap(cast(subkey as uint32))) over w1 as sum1,
    COUNT(*)                            over w1 as count1,
    ListSort(AGGREGATE_LIST_DISTINCT(subkey) over w1) as agglist_distinct1,

    SUM(cast(subkey as uint32))         over w2 as sum2,
    AGGREGATE_LIST(subkey)              over w2 as agglist2,
FROM plato.Input
WINDOW
    w1 as (PARTITION BY key ORDER BY value ROWS BETWEEN 5 PRECEDING AND 10 PRECEDING),
    w2 as (                                ROWS BETWEEN 3 FOLLOWING AND 2 FOLLOWING)
ORDER BY value;
