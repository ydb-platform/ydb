PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: TzDate('2024-01-01,Europe/Moscow'), count: 2|>,
    <|a: TzDate('2024-01-02,Europe/Moscow'), count: 3|>,
    <|a: TzDate('2024-01-03,Europe/Moscow'), count: 2|>,
    <|a: TzDate('2024-01-05,Europe/Moscow'), count: 2|>,
    <|a: TzDate('2024-01-06,Europe/Moscow'), count: 2|>,
];

$win_result = (
    SELECT
        COUNT(*) OVER w1 AS actual_count,
        count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            ORDER BY
                a ASC
            RANGE BETWEEN Interval64('P1DT12H') PRECEDING AND Interval64('P1D') FOLLOWING
        )
);

$str = ($x) -> {
    RETURN CAST($x AS String) ?? 'null';
};

SELECT
    Ensure(count, count IS NOT DISTINCT FROM actual_count, $str(actual_count))
FROM
    $win_result
;
