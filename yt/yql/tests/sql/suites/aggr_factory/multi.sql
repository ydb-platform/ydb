/* syntax version 1 */
/* postgres can not */
USE plato;

$input = (select AsStruct(
    key as key,
    Just(subkey) as subkey,
    Just(value) as value) as nums from Input);

SELECT
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("count")) as count,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("min")) as min,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("max")) as max,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("sum")) as sum,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("avg")) as avg,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("stddev")) as stddev,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("percentile", 0.5)) as p50,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("aggregate_list")) as agg_list,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("aggregate_list_distinct")) as agg_list_distinct,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("mode")) as mode,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("top", 3)) as top,
FROM $input;
