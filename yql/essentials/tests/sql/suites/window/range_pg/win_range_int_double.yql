PRAGMA WindowNewPipeline;

$data = [
    <|a: pgfloat4('1.5'), count: 2|>,
    <|a: pgfloat4('2.0'), count: 3|>,
    <|a: pgfloat4('2.8'), count: 3|>,
    <|a: pgfloat4('5.0'), count: 2|>,
    <|a: pgfloat4('6.0'), count: 2|>,
];

$win_result = (
    SELECT
        COUNT(*) OVER w1 AS actual_count,
        count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            ORDER BY
                a ASC
            RANGE BETWEEN pgfloat8('1.5') PRECEDING AND pgfloat8('1') FOLLOWING
        )
);

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
