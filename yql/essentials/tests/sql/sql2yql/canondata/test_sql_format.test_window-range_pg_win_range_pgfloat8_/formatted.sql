PRAGMA WindowNewPipeline;

$data = [
    <|a: pgfloat8('-10.5'), b: 1, sum11: pgfloat8('-10.5'), count1: 1, sum22: NULL, count2: 0|>,
    <|a: pgfloat8('-5.0'), b: 1, sum11: pgfloat8('-15.5'), count1: 2, sum22: NULL, count2: 0|>,
    <|a: pgfloat8('0.0'), b: 1, sum11: pgfloat8('-5.0'), count1: 2, sum22: pgfloat8('-5.0'), count2: 1|>,
];

$win_result = (
    SELECT
        pg::sum(a) OVER w1 AS actual_sum11,
        COUNT(*) OVER w1 AS actual_count1,
        pg::sum(a) OVER w2 AS actual_sum22,
        COUNT(*) OVER w2 AS actual_count2,
        sum11,
        count1,
        sum22,
        count2,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN pgfloat8('10.0') PRECEDING AND CURRENT ROW
        ),
        w2 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN pgfloat8('5.0') PRECEDING AND pgfloat8('0.5') PRECEDING
        )
);

SELECT
    Ensure(actual_sum11, sum11 IS NOT DISTINCT FROM actual_sum11),
    Ensure(actual_count1, count1 IS NOT DISTINCT FROM actual_count1),
    Ensure(actual_sum22, sum22 IS NOT DISTINCT FROM actual_sum22),
    Ensure(actual_count2, count2 IS NOT DISTINCT FROM actual_count2)
FROM
    $win_result
;
