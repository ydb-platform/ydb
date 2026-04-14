PRAGMA WindowNewPipeline;

$data = [
    <|a: pginterval('1 day 2 hours 3 minutes 4.567888 seconds'), b: 1, count: 2|>,
    <|a: pginterval('1 day 2 hours 3 minutes 4.567889 seconds'), b: 1, count: 1|>,
    <|a: pginterval('1 day 2 hours 3 minutes 4.567890 seconds'), b: 1, count: 0|>,
    <|a: NULL, b: 1, count: 5|>,
    <|a: NULL, b: 1, count: 5|>,
];

$win_result = (
    SELECT
        COUNT(*) OVER w1 AS actual_count,
        count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN pginterval('0.000001 seconds') FOLLOWING AND UNBOUNDED FOLLOWING
        )
);

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
