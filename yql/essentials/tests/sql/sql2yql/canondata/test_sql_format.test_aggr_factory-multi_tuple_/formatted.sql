/* syntax version 1 */
/* postgres can not */
$input = (
    SELECT
        AsTuple(
            1,
            Just(2),
            Just(3)
        ) AS nums
    UNION ALL
    SELECT
        AsTuple(
            4,
            Just(5),
            Just(6)
        ) AS nums
);

SELECT
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY('count')) AS count,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY('min')) AS min,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY('max')) AS max,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY('sum')) AS sum,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY('avg')) AS avg,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY('stddev')) AS stddev,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY('percentile', 0.5)) AS p50,
    MULTI_AGGREGATE_BY(nums, AGGREGATION_FACTORY('aggregate_list')) AS agg_list,
FROM
    $input
;
