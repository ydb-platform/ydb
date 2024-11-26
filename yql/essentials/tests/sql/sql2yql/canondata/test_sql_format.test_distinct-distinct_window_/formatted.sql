/* syntax version 1 */
USE plato;

SELECT DISTINCT
    AGGREGATE_LIST(value) OVER w AS values,
    key
FROM Input2
WINDOW
    w AS (
        PARTITION BY
            key
        ORDER BY
            value
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
ORDER BY
    key;
