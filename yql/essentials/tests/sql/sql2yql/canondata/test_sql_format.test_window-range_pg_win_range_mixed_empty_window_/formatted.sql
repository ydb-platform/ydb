PRAGMA WindowNewPipeline;

$data = [
    <|a: pgfloat4('1'), expected_count: 0, expected_sum: NULL|>,
    <|a: pgfloat4('2'), expected_count: 0, expected_sum: NULL|>,
    <|a: pgfloat4('3'), expected_count: 0, expected_sum: NULL|>,
    <|a: pgfloat4('4'), expected_count: 0, expected_sum: NULL|>,
    <|a: pgfloat4('5'), expected_count: 0, expected_sum: NULL|>,
];

$win_result = (
    SELECT
        a,
        COUNT(*) OVER w1 AS count_w1,
        pg::sum(a) OVER w2 AS sum_w1,
        expected_count,
        expected_sum,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            ORDER BY
                a ASC
            RANGE BETWEEN pgfloat8('1.0') FOLLOWING AND pgfloat8('0') FOLLOWING
        ),
        w2 AS (
            ORDER BY
                a ASC
            RANGE BETWEEN pgfloat8('1000000') FOLLOWING AND pgfloat8('1000') FOLLOWING
        )
);

SELECT
    Ensure(count_w1, count_w1 IS NOT DISTINCT FROM expected_count),
    Ensure(sum_w1, sum_w1 IS NOT DISTINCT FROM Nothing(pgfloat4)),
FROM
    $win_result
;
