PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: NULL, b: 1, sum: NULL, count: 2|>,
    <|a: NULL, b: 1, sum: NULL, count: 2|>,
    <|a: uint16('100'), b: 1, sum: uint16('100'), count: 3|>,
    <|a: uint16('200'), b: 1, sum: uint16('300'), count: 4|>,
    <|a: uint16('250'), b: 1, sum: uint16('550'), count: 5|>,
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
            RANGE BETWEEN UNBOUNDED PRECEDING AND uint16('10') FOLLOWING
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
