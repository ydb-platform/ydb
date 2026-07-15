PRAGMA WindowNewPipeline;

$data = [
    <|a: NULL, b: 1, sum11: NULL, count: 2|>,
    <|a: NULL, b: 1, sum11: NULL, count: 2|>,
    <|a: pgnumeric('1000000'), b: 1, sum11: pgnumeric('1000000'), count: 1|>,
    <|a: pgnumeric('2000000'), b: 1, sum11: pgnumeric('3000000'), count: 1|>,
    <|a: pgnumeric('3000000'), b: 1, sum11: pgnumeric('6000000'), count: 1|>,
];

$win_result = (
    SELECT
        COUNT(*) OVER w1 AS actual_count1,
        COUNT(*) OVER w2 AS actual_count2,
        COUNT(*) OVER w3 AS actual_count3,
        count AS count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN pgfloat8('1.0') PRECEDING AND 0p FOLLOWING
        ),
        w2 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN pgfloat8('1.0') PRECEDING AND 8p FOLLOWING
        ),
        w3 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN pgfloat4('1.0') PRECEDING AND 4p FOLLOWING
        )
);

SELECT
    Ensure(actual_count1, actual_count1 IS NOT DISTINCT FROM count),
    Ensure(actual_count2, actual_count2 IS NOT DISTINCT FROM count),
    Ensure(actual_count3, actual_count3 IS NOT DISTINCT FROM count)
FROM
    $win_result
;
