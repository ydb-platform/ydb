PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: 'apple', c: 1, b: 1, count: 1|>,
    <|a: 'apple', c: 2, b: 1, count: 2|>,
    <|a: 'banana', c: 1, b: 1, count: 3|>,
    <|a: 'banana', c: 2, b: 1, count: 4|>,
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
                a ASC,
                c ASC
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
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
