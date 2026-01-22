PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: int64('-1'), b: 1, sum: int64('-3'), count: 2|>,
    <|a: int64('-2'), b: 1, sum: int64('-6'), count: 3|>,
    <|a: int64('-3'), b: 1, sum: int64('-5'), count: 2|>,
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
            PARTITION BY
                b
            ORDER BY
                a * 5 ASC
            RANGE BETWEEN 5l PRECEDING AND 5l FOLLOWING
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
