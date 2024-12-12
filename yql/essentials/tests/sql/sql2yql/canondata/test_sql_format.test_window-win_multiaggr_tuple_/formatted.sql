/* postgres can not */
USE plato;

SELECT
    MULTI_AGGREGATE_BY(AsTuple(subkey, value), AGGREGATION_FACTORY("count")) OVER w,
    MULTI_AGGREGATE_BY(AsTuple(subkey, value), AGGREGATION_FACTORY("max")) OVER w
FROM
    Input
WINDOW
    w AS (
        ORDER BY
            key
    )
;
