PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: NULL, b: 1, sum: NULL, count: 2|>,
    <|a: NULL, b: 1, sum: NULL, count: 2|>,
    <|a: uint8('8'), b: 1, sum: uint8('8'), count: 1|>,
    <|a: uint8('10'), b: 1, sum: uint8('10'), count: 1|>,
    <|a: uint8('11'), b: 1, sum: uint8('21'), count: 2|>,
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
            RANGE BETWEEN uint8('1') PRECEDING AND CURRENT ROW
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
