PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: int16('-1000'), b: 1, sum: int16('-1500'), count: 5|>,
    <|a: int16('-500'), b: 1, sum: int16('-500'), count: 4|>,
    <|a: int16('0'), b: 1, sum: int16('0'), count: 3|>,
    <|a: NULL, b: 1, sum: NULL, count: 2|>,
    <|a: NULL, b: 1, sum: NULL, count: 2|>,
];

$win_result = (
    SELECT
        SUM(a) OVER w1 AS actual_sum,
        COUNT(*) OVER w1 AS actual_count,
        sum,
        count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                -a DESC
            RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        )
);

$str = ($x) -> {
    RETURN CAST($x AS String) ?? 'null';
};

SELECT
    Ensure(sum, sum IS NOT DISTINCT FROM actual_sum, $str(actual_sum)),
    Ensure(count, count IS NOT DISTINCT FROM actual_count, $str(actual_count))
FROM
    $win_result
;
