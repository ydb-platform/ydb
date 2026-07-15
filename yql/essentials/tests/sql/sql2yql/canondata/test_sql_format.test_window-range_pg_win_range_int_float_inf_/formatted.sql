PRAGMA WindowNewPipeline;

$data = [
    <|a: pgfloat4('1'), count: 5|>,
    <|a: pgfloat4('2'), count: 5|>,
    <|a: pgfloat4('3'), count: 5|>,
    <|a: pgfloat4('5'), count: 5|>,
    <|a: pgfloat4('6'), count: 5|>,
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
            RANGE BETWEEN pgfloat8('Inf') PRECEDING AND pgfloat8('Inf') FOLLOWING
        )
);

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
