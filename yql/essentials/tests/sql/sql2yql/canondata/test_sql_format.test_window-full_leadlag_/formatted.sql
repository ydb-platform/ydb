/* syntax version 1 */
/* postgres can not */
$in = (
    SELECT
        value,
        SUM(unwrap(CAST(subkey AS uint32))) OVER w1 AS sum1,
        LEAD(value || value, 3) OVER w1 AS dvalue_lead1,
        SUM(CAST(subkey AS uint32)) OVER w2 AS sum2,
        LAG(CAST(value AS uint32)) OVER w2 AS value_lag2,
    FROM (
        SELECT
            *
        FROM
            plato.Input
        WHERE
            key == '1'
    )
    WINDOW
        w1 AS (
            ORDER BY
                value
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ),
        w2 AS (
            PARTITION BY
                key
            ORDER BY
                value DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
);

SELECT
    value,
    dvalue_lead1,
    value_lag2
FROM
    $in
ORDER BY
    value
;
