/* syntax version 1 */
/* postgres can not */
SELECT
    min(x),
    count(x),
    count(*),
    aggregate_list_distinct(x),
    aggregate_list(x),
    bool_and(x)
FROM (
    SELECT
        NULL AS x
    UNION ALL
    SELECT
        NULL AS x
);

SELECT
    min(x),
    count(x),
    count(*),
    aggregate_list_distinct(x),
    aggregate_list(x),
    bool_and(x)
FROM (
    SELECT
        NULL AS x,
        1 AS y
    UNION ALL
    SELECT
        NULL AS x,
        2 AS y
)
GROUP BY
    y
;
