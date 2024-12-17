/* syntax version 1 */
/* postgres can not */
PRAGMA warning('disable', '4520');

SELECT
    value,
    SUM(unwrap(CAST(subkey AS uint32))) OVER w1 AS sum1,
    LEAD(value || value, 3) OVER w1 AS dvalue_lead1,
    SUM(CAST(subkey AS uint32)) OVER w2 AS sum2,
    LAG(CAST(value AS uint32)) OVER w2 AS value_lag2,
FROM
    plato.Input
WINDOW
    w1 AS (
        PARTITION BY
            key
        ORDER BY
            value
        ROWS BETWEEN 5 PRECEDING AND 10 PRECEDING
    ),
    w2 AS (
        ORDER BY
            value DESC
        ROWS BETWEEN 3 FOLLOWING AND 2 FOLLOWING
    )
ORDER BY
    value
;
