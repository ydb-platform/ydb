/* postgres can not */
USE plato;

SELECT
    MULTI_AGGREGATE_BY(AsList(subkey, value), AGGREGATION_FACTORY("count")) OVER w,
    MULTI_AGGREGATE_BY(AsList(subkey, value), AGGREGATION_FACTORY("max")) OVER w
FROM Input
WINDOW
    w AS (
        ORDER BY
            key
    );
