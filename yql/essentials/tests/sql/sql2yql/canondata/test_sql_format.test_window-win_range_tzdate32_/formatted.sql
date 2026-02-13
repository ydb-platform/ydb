PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: TzDate32('2017-11-25,Europe/Moscow'), b: 1, count: 0|>,
    <|a: TzDate32('2017-11-26,Europe/Moscow'), b: 1, count: 1|>,
    <|a: TzDate32('2017-11-27,Europe/Moscow'), b: 1, count: 2|>,
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
            RANGE BETWEEN Interval64('P3D') PRECEDING AND Interval64('P1D') PRECEDING
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
