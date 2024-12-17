/* syntax version 1 */
/* postgres can not */
$input = (
    SELECT
        AsList(
            1,
            2,
            3
        ) AS nums
    UNION ALL
    SELECT
        AsList(
            4,
            5
        ) AS nums
    UNION ALL
    SELECT
        AsList(
            1,
            2,
            3
        ) AS nums
);

SELECT
    MULTI_AGGREGATE_BY(DISTINCT ListExtend(nums, AsList(1, 5)), AGGREGATION_FACTORY('count')) AS count,
    MULTI_AGGREGATE_BY(DISTINCT ListExtend(nums, AsList(1, 5)), AGGREGATION_FACTORY('min')) AS min,
    MULTI_AGGREGATE_BY(DISTINCT ListExtend(nums, AsList(1, 5)), AGGREGATION_FACTORY('max')) AS max,
    MULTI_AGGREGATE_BY(DISTINCT ListExtend(nums, AsList(1, 5)), AGGREGATION_FACTORY('sum')) AS sum,
    MULTI_AGGREGATE_BY(DISTINCT ListExtend(nums, AsList(1, 5)), AGGREGATION_FACTORY('percentile', 0.5)) AS p50,
    MULTI_AGGREGATE_BY(DISTINCT ListExtend(nums, AsList(1, 5)), AGGREGATION_FACTORY('aggregate_list')) AS agg_list
FROM
    $input
;
