PRAGMA WindowNewPipeline;

$data = [
    <|a: pgint2('-1000'), b: 1, sum11: pgint8('-1500'), count: 5|>,
    <|a: pgint2('-500'), b: 1, sum11: pgint8('-500'), count: 4|>,
    <|a: pgint2('0'), b: 1, sum11: pgint8('0'), count: 3|>,
    <|a: NULL, b: 1, sum11: NULL, count: 2|>,
    <|a: NULL, b: 1, sum11: NULL, count: 2|>,
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
                -a DESC
            RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        )
);

SELECT
    Ensure(actual_sum11, sum11 IS NOT DISTINCT FROM actual_sum11),
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
