PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$uint64_max = uint64('18446744073709551615');

$data = [
    <|a: $uint64_max, count: 4|>,
    <|a: $uint64_max, count: 4|>,
    <|a: 0, count: 4|>,
    <|a: 0, count: 4|>,
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
            RANGE BETWEEN $uint64_max PRECEDING AND $uint64_max FOLLOWING
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
