PRAGMA WindowNewPipeline;

$data = [
    <|a: pgfloat4('1'), count: 1|>,
    <|a: pgfloat4('5'), count: 2|>,
    <|a: pgfloat4('6'), count: 3|>,
    <|a: pgfloat4('7'), count: 3|>,
    <|a: pgfloat4('9'), count: 3|>,
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
            RANGE BETWEEN pgfloat8('3') PRECEDING AND pgfloat8('1.2') FOLLOWING
        )
);

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
