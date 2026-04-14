PRAGMA WindowNewPipeline;

$data = [
    <|a: NULL, b: 1, sum11: pgnumeric('-1500000'), count: 5|>,
    <|a: NULL, b: 1, sum11: pgnumeric('-1500000'), count: 5|>,
    <|a: pgint8('-1000000'), b: 1, sum11: pgnumeric('-1500000'), count: 3|>,
    <|a: pgint8('-500000'), b: 1, sum11: pgnumeric('-500000'), count: 2|>,
    <|a: pgint8('0'), b: 1, sum11: pgnumeric('0'), count: 1|>,
];

$win_result = (
    SELECT
        pg::sum(a) OVER w1 AS actual_sum11,
        COUNT(*) OVER w1 AS actual_count,
        sum11,
        count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        )
);

SELECT
    Ensure(actual_sum11, sum11 IS NOT DISTINCT FROM actual_sum11),
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
