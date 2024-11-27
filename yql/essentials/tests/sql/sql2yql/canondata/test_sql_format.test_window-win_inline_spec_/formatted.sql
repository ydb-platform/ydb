/* syntax version 1 */
SELECT
    key,
    max(key) OVER (
        ORDER BY
            key
    ) AS running_max,
    lead(key) OVER (
        ORDER BY
            key
        ROWS UNBOUNDED PRECEDING
    ) AS next_key,
    aggregate_list(key) OVER w AS keys,
FROM plato.Input
WINDOW
    w AS (
        ORDER BY
            key
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
ORDER BY
    key;
