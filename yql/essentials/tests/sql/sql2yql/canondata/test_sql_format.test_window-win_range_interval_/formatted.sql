PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: Interval('P1DT2H3M4.567888S'), b: 1, count: 2|>,
    <|a: Interval('P1DT2H3M4.567889S'), b: 1, count: 1|>,
    <|a: Interval('P1DT2H3M4.567890S'), b: 1, count: 0|>,
    <|a: NULL, b: 1, count: 5|>,
    <|a: NULL, b: 1, count: 5|>,
];

$win_result = (
    SELECT
        COUNT(*) OVER w1 AS actual_count,
        count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN Interval('PT0.000001S') FOLLOWING AND UNBOUNDED FOLLOWING
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
