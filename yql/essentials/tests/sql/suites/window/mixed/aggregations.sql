/* syntax version 1 */
/* postgres can not */

SELECT
    value,

    SUM(unwrap(cast(subkey as uint32))) over w1 as sum1,
    AGGREGATE_LIST(subkey)              over w1 as agglist1,

    SUM(cast(subkey as uint32))         over w2 as sum2,
    AGGREGATE_LIST(subkey)              over w2 as agglist2,

FROM (SELECT * FROM plato.Input WHERE key = '1')
WINDOW
    w1 as (PARTITION BY key ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
    w2 as (PARTITION BY key ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
ORDER BY value;
