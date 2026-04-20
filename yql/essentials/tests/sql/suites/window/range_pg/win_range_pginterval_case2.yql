PRAGMA WindowNewPipeline;

$data = [
    <|a: pginterval('1 day 2 hours 3 minutes 4.567888 seconds'), b: 1, count1: 1, count2: 2|>,
    <|a: pginterval('1 day 2 hours 3 minutes 4.567889 seconds'), b: 1, count1: 2, count2: 2|>,
    <|a: pginterval('1 day 2 hours 3 minutes 4.567890 seconds'), b: 1, count1: 2, count2: 1|>,
    <|a: NULL, b: 1, count1: 2, count2: 2|>,
    <|a: NULL, b: 1, count1: 2, count2: 2|>,
];

$win_result = (
    SELECT
        COUNT(*) OVER w1 AS actual_count1,
        COUNT(*) OVER w2 AS actual_count2,
        count1,
        count2,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN pginterval('0.000001 seconds') PRECEDING AND CURRENT ROW
        ),
        w2 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN CURRENT ROW AND pginterval('0.000001 seconds') FOLLOWING
        )
);

SELECT
    Ensure(actual_count1, count1 IS NOT DISTINCT FROM actual_count1),
    Ensure(actual_count2, count2 IS NOT DISTINCT FROM actual_count2)
FROM
    $win_result
;
