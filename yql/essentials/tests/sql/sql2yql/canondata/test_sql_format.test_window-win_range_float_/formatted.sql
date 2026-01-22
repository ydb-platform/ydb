PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: float('1.5'), b: 1, sum: float('1.5'), count: 1|>,
    <|a: float('2.0'), b: 1, sum: float('3.5'), count: 2|>,
    <|a: float('2.8'), b: 1, sum: float('6.3'), count: 3|>,
    <|a: float('5.0'), b: 1, sum: float('5.0'), count: 1|>,
    <|a: float('6.0'), b: 1, sum: float('11.0'), count: 2|>,
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
                a ASC
            RANGE BETWEEN float('1.5') PRECEDING AND CURRENT ROW
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
