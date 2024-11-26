/* syntax version 1 */
/* postgres can not */

$input = 
select AsTuple(
    1,
    Just(2),
    Just(3)) as nums
union all
select AsTuple(4,
    Just(5),
    Just(6)) as nums;

SELECT
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("count")) as count,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("min")) as min,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("max")) as max,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("sum")) as sum,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("avg")) as avg,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("stddev")) as stddev,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("percentile", 0.5)) as p50,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY("aggregate_list")) as agg_list,
FROM $input;
