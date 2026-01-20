PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: int32('-50000'), b: 1, sum1: int32('-50000'), count1: 1, sum2: NULL, count2: 0|>,
    <|a: int32('-10000'), b: 1, sum1: int32('-60000'), count1: 2, sum2: int32('-50000'), count2: 1|>,
    <|a: int32('0'), b: 1, sum1: int32('-60000'), count1: 3, sum2: int32('-10000'), count2: 1|>,
    <|a: NULL, b: 1, sum1: NULL, count1: 2, sum2: NULL, count2: 2|>,
    <|a: NULL, b: 1, sum1: NULL, count1: 2, sum2: NULL, count2: 2|>,
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
            RANGE BETWEEN int32('50000') PRECEDING AND CURRENT ROW
        ),
        w2 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN int32('40000') PRECEDING AND int32('10000') PRECEDING
        )
);

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
