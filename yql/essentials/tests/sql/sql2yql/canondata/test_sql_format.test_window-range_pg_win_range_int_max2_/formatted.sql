PRAGMA WindowNewPipeline;

$big_val = pgnumeric('18446744073709551615');

$data = [
    <|a: $big_val, count: 4|>,
    <|a: $big_val, count: 4|>,
    <|a: pgnumeric('0'), count: 4|>,
    <|a: pgnumeric('0'), count: 4|>,
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
            RANGE BETWEEN $big_val PRECEDING AND $big_val FOLLOWING
        )
);

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
