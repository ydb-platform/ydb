/* syntax version 1 */
/* postgres can not */
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
    ),
    w2 AS (
        ORDER BY
            value
    )
ORDER BY
    value
;
