/* syntax version 1 */
/* postgres can not */
SELECT
    value,
    SUM(unwrap(CAST(subkey AS uint32))) OVER w1 AS sum1,
    COUNT(*) OVER w1 AS count1,
    ListSort(AGGREGATE_LIST_DISTINCT(subkey) OVER w1) AS agglist_distinct1,
    SUM(CAST(subkey AS uint32)) OVER w2 AS sum2,
    AGGREGATE_LIST(subkey) OVER w2 AS agglist2,
FROM (
    SELECT
        *
    FROM plato.Input
    WHERE key == '1'
)
WINDOW
    w1 AS (
        PARTITION BY
            key
        ORDER BY
            value
        ROWS BETWEEN UNBOUNDED PRECEDING AND 3 FOLLOWING
    ),
    w2 AS (
        PARTITION BY
            key
        ORDER BY
            value
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING
    )
ORDER BY
    value;
