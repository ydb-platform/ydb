PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: TzDatetime64('2017-11-27T13:22:00,America/Los_Angeles'), b: 1, count: 3|>,
    <|a: TzDatetime64('2017-11-27T13:23:00,America/Los_Angeles'), b: 1, count: 3|>,
    <|a: TzDatetime64('2017-11-27T13:24:00,America/Los_Angeles'), b: 1, count: 2|>,
    <|a: TzDatetime64('2017-11-27T13:22:00,America/Los_Angeles'), b: 2, count: 3|>,
    <|a: TzDatetime64('2017-11-27T13:23:00,America/Los_Angeles'), b: 2, count: 3|>,
    <|a: TzDatetime64('2017-11-27T13:24:00,America/Los_Angeles'), b: 2, count: 2|>,
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
            RANGE BETWEEN Interval64('PT1M') PRECEDING AND Interval64('PT3M') FOLLOWING
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
