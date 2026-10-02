PRAGMA WindowNewPipeline;

$data = [
    <|a: pgint2('-128'), count: 3|>,
    <|a: pgint2('0'), count: 3|>,
    <|a: pgint2('127'), count: 3|>,
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
            RANGE BETWEEN pgint2('32767') PRECEDING AND pgint2('32767') FOLLOWING
        )
);

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
