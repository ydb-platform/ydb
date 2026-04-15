PRAGMA WindowNewPipeline;

$data = [
    <|a: pgdate('2024-01-01'), count: 2|>,
    <|a: pgdate('2024-01-02'), count: 3|>,
    <|a: pgdate('2024-01-03'), count: 2|>,
    <|a: pgdate('2024-01-05'), count: 2|>,
    <|a: pgdate('2024-01-06'), count: 2|>,
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
            RANGE BETWEEN pginterval('1 day 12 hours') PRECEDING AND pginterval('1 day') FOLLOWING
        )
);

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
