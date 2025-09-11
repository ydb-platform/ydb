/* syntax version 1 */
/* postgres can not */

SELECT
    value,
    SUM(unwrap(cast(subkey as uint32))) over w1 as sum1,
    LEAD(value || value, 3)             over w1 as dvalue_lead1,

    SUM(cast(subkey as uint32))         over w2 as sum2,
    LAG(cast(value as uint32))          over w2 as value_lag2,
FROM plato.Input
WINDOW
    w1 as (PARTITION BY key ORDER BY value),
    w2 as (                 ORDER BY value)
ORDER BY value;
