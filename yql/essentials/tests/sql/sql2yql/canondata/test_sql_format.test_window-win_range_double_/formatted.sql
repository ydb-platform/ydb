PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: double('-10.5'), b: 1, sum1: double('-10.5'), count1: 1, sum2: NULL, count2: 0|>,
    <|a: double('-5.0'), b: 1, sum1: double('-15.5'), count1: 2, sum2: NULL, count2: 0|>,
    <|a: double('0.0'), b: 1, sum1: double('-5.0'), count1: 2, sum2: double('-5.0'), count2: 1|>,
];

$win_result = (
    SELECT
        SUM(a) OVER w1 AS actual_sum1,
        COUNT(*) OVER w1 AS actual_count1,
        SUM(a) OVER w2 AS actual_sum2,
        COUNT(*) OVER w2 AS actual_count2,
        sum1,
        count1,
        sum2,
        count2,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN double('10.0') PRECEDING AND CURRENT ROW
        ),
        w2 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN double('5.0') PRECEDING AND double('0.5') PRECEDING
        )
);

$str = ($x) -> {
    RETURN CAST($x AS String) ?? 'null';
};

$str = ($x) -> {
    RETURN CAST($x AS String) ?? 'null';
};

SELECT
    Ensure(sum1, sum1 IS NOT DISTINCT FROM actual_sum1, $str(actual_sum1)),
    Ensure(count1, count1 IS NOT DISTINCT FROM actual_count1, $str(actual_count1)),
    Ensure(sum2, sum2 IS NOT DISTINCT FROM actual_sum2, $str(actual_sum2)),
    Ensure(count2, count2 IS NOT DISTINCT FROM actual_count2, $str(actual_count2))
FROM
    $win_result
;
