/* syntax version 1 */
/* postgres can not */
SELECT
    min(x) OVER w,
    count(x) OVER w,
    count(*) OVER w,
    aggregate_list_distinct(x) OVER w,
    aggregate_list(x) OVER w,
    bool_and(x) OVER w
FROM (
    SELECT
        NULL AS x
    UNION ALL
    SELECT
        NULL AS x
)
WINDOW
    w AS (
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
;

SELECT
    min(x) OVER w,
    count(x) OVER w,
    count(*) OVER w,
    aggregate_list_distinct(x) OVER w,
    aggregate_list(x) OVER w,
    bool_and(x) OVER w
FROM (
    SELECT
        NULL AS x
    UNION ALL
    SELECT
        NULL AS x
)
WINDOW
    w AS (
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
;
