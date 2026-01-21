PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: Timestamp('2017-11-27T13:24:00.123454Z'), b: 1, count: 5|>,
    <|a: Timestamp('2017-11-27T13:24:00.123455Z'), b: 1, count: 5|>,
    <|a: Timestamp('2017-11-27T13:24:00.123456Z'), b: 1, count: 5|>,
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
            RANGE BETWEEN CURRENT ROW AND CURRENT ROW
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
