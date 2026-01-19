PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: Timestamp64('2017-11-27T13:24:00.123454Z'), b: 1, count: 1|>,
    <|a: Timestamp64('2017-11-27T13:24:00.123455Z'), b: 1, count: 2|>,
    <|a: Timestamp64('2017-11-27T13:24:00.123456Z'), b: 1, count: 2|>,
    <|a: Timestamp64('2017-11-27T13:24:00.123457Z'), b: 1, count: 2|>,
    <|a: Timestamp64('2017-11-27T13:24:00.123458Z'), b: 1, count: 2|>,
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
            RANGE BETWEEN Interval64('PT0.000001S') PRECEDING AND CURRENT ROW
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
