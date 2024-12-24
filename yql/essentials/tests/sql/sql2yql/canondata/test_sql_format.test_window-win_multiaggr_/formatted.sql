/* postgres can not */
USE plato;

SELECT
    MULTI_AGGREGATE_BY(AsStruct(subkey AS a, value AS b), AGGREGATION_FACTORY('count')) OVER w,
    MULTI_AGGREGATE_BY(AsStruct(subkey AS a, value AS b), AGGREGATION_FACTORY('max')) OVER w
FROM
    Input
WINDOW
    w AS (
        ORDER BY
            key
    )
;
