/* syntax version 1 */
/* postgres can not */
USE plato;

$input = (
    SELECT
        AsStruct(
            key AS key,
            Just(subkey) AS subkey,
            Just(value) AS value
        ) AS nums
    FROM Input
);

SELECT
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("count")) AS count,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("min")) AS min,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("max")) AS max,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("sum")) AS sum,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("avg")) AS avg,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("stddev")) AS stddev,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("percentile", 0.5)) AS p50,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("aggregate_list")) AS agg_list,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("aggregate_list_distinct")) AS agg_list_distinct,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("mode")) AS mode,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("top", 3)) AS top,
FROM $input;
