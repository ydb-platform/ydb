PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: Interval64('P1DT2H3M4.567888S'), b: 1, count1: 1, count2: 2|>,
    <|a: Interval64('P1DT2H3M4.567889S'), b: 1, count1: 2, count2: 2|>,
    <|a: Interval64('P1DT2H3M4.567890S'), b: 1, count1: 2, count2: 1|>,
    <|a: NULL, b: 1, count1: 2, count2: 2|>,
    <|a: NULL, b: 1, count1: 2, count2: 2|>,
];

$win_result = (
    SELECT
        COUNT(*) OVER w1 AS actual_count1,
        COUNT(*) OVER w2 AS actual_count2,
        count1,
        count2,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN Interval64('PT0.000001S') PRECEDING AND CURRENT ROW
        ),
        w2 AS (
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
    Ensure(count1, count1 IS NOT DISTINCT FROM actual_count1, $str(actual_count1)),
    Ensure(count2, count2 IS NOT DISTINCT FROM actual_count2, $str(actual_count2))
FROM
    $win_result
;
