/* syntax version 1 */
/* postgres can not */

$input = 
select AsList(
    1,
    2,
    3
) as nums
union all
select AsList(
    4,
    5) as nums
union all
select AsList(
    1,
    2,
    3
) as nums;

SELECT
    MULTI_AGGREGATE_BY(distinct ListExtend(nums, AsList(1,5)), AGGREGATION_FACTORY("count")) as count,
    MULTI_AGGREGATE_BY(distinct ListExtend(nums, AsList(1,5)), AGGREGATION_FACTORY("min")) as min,
    MULTI_AGGREGATE_BY(distinct ListExtend(nums, AsList(1,5)), AGGREGATION_FACTORY("max")) as max,
    MULTI_AGGREGATE_BY(distinct ListExtend(nums, AsList(1,5)), AGGREGATION_FACTORY("sum")) as sum,
    MULTI_AGGREGATE_BY(distinct ListExtend(nums, AsList(1,5)), AGGREGATION_FACTORY("percentile", 0.5)) as p50,
    MULTI_AGGREGATE_BY(distinct ListExtend(nums, AsList(1,5)), AGGREGATION_FACTORY("aggregate_list")) as agg_list
FROM $input;
