PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: TzTimestamp64('2017-11-27T13:24:00.123454,GMT'), b: 1, count: 2|>,
    <|a: TzTimestamp64('2017-11-27T13:24:00.123455,GMT'), b: 1, count: 1|>,
    <|a: TzTimestamp64('2017-11-27T13:24:00.123459,GMT'), b: 1, count: 1|>,
    <|a: NULL, b: 1, count: 2|>,
    <|a: NULL, b: 1, count: 2|>,
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
            RANGE BETWEEN CURRENT ROW AND Interval64('PT0.000001S') FOLLOWING
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
