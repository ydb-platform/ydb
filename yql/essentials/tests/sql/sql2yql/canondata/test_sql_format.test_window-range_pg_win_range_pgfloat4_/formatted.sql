PRAGMA WindowNewPipeline;

$data = [
    <|a: pgfloat4('1.5'), b: 1, sum11: pgfloat4('1.5'), count: 1|>,
    <|a: pgfloat4('2.0'), b: 1, sum11: pgfloat4('3.5'), count: 2|>,
    <|a: pgfloat4('2.8'), b: 1, sum11: pgfloat4('6.3'), count: 3|>,
    <|a: pgfloat4('5.0'), b: 1, sum11: pgfloat4('5.0'), count: 1|>,
    <|a: pgfloat4('6.0'), b: 1, sum11: pgfloat4('11.0'), count: 2|>,
];

$win_result = (
    SELECT
        pg::sum(a) OVER w1 AS actual_sum11,
        COUNT(*) OVER w1 AS actual_count,
        sum11,
        count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN pgfloat8('1.5') PRECEDING AND CURRENT ROW
        )
);

SELECT
    Ensure(actual_sum11, sum11 IS NOT DISTINCT FROM actual_sum11),
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
