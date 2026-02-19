PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

-- Test that different zero types (int, uint8, float) all produce the same result
-- With 0 preceding/following, each row should only count itself (count = 1)
$data = [
    <|a: 1, count: 1|>,
    <|a: 2, count: 1|>,
    <|a: 3, count: 1|>,
    <|a: 5, count: 1|>,
    <|a: 6, count: 1|>,
];

-- Test with 0 as int (preceding)
$win_result_int_preceding = (
    SELECT
        COUNT(*) OVER w1 AS actual_count,
        count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            ORDER BY
                a ASC
            RANGE BETWEEN 0 PRECEDING AND 0 FOLLOWING
        )
);

-- Test with 0 as uint8 (preceding)
$win_result_uint8_preceding = (
    SELECT
        COUNT(*) OVER w1 AS actual_count,
        count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            ORDER BY
                a ASC
            RANGE BETWEEN Uint8('0') PRECEDING AND Uint8('0') FOLLOWING
        )
);

-- Test with 0.0 as float
$win_result_float = (
    SELECT
        COUNT(*) OVER w1 AS actual_count,
        count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            ORDER BY
                a ASC
            RANGE BETWEEN 0.0 PRECEDING AND 0.0 FOLLOWING
        )
);

$str = ($x) -> {
    RETURN CAST($x AS String) ?? 'null';
};

SELECT
    Ensure(count, count IS NOT DISTINCT FROM actual_count, 'int_preceding: ' || $str(actual_count))
FROM
    $win_result_int_preceding
;

SELECT
    Ensure(count, count IS NOT DISTINCT FROM actual_count, 'uint8_preceding: ' || $str(actual_count))
FROM
    $win_result_uint8_preceding
;

SELECT
    Ensure(count, count IS NOT DISTINCT FROM actual_count, 'float: ' || $str(actual_count))
FROM
    $win_result_float
;
