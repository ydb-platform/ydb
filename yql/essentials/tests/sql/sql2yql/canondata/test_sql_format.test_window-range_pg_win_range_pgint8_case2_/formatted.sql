PRAGMA WindowNewPipeline;

$data = [
    <|a: pgint8('1000'), b: 1, sum11: pgnumeric('1000'), count: 1|>,
    <|a: pgint8('2000'), b: 1, sum11: pgnumeric('3000'), count: 2|>,
    <|a: pgint8('2500'), b: 1, sum11: pgnumeric('4500'), count: 2|>,
    <|a: pgint8('3000'), b: 1, sum11: pgnumeric('7500'), count: 3|>,
    <|a: pgint8('5000'), b: 1, sum11: pgnumeric('5000'), count: 1|>,
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
            RANGE BETWEEN pgint8('1000') PRECEDING AND CURRENT ROW
        )
);

SELECT
    Ensure(actual_sum11, sum11 IS NOT DISTINCT FROM actual_sum11),
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
