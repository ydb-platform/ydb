PRAGMA WindowNewPipeline;

$data = [
    <|a: pginterval('1 day'), count: 2|>,
    <|a: pginterval('2 days'), count: 3|>,
    <|a: pginterval('3 days'), count: 2|>,
    <|a: pginterval('5 days'), count: 2|>,
    <|a: pginterval('6 days'), count: 2|>,
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
