PRAGMA WindowNewPipeline;

$data = [
    <|a: NULL, b: 1, count1: 5, count2: 5|>,
    <|a: NULL, b: 1, count1: 5, count2: 5|>,
    <|a: pgint2('1000'), b: 1, count1: 3, count2: 3|>,
    <|a: pgint2('2000'), b: 1, count1: 2, count2: 3|>,
    <|a: pgint2('3000'), b: 1, count1: 1, count2: 3|>,
    <|a: pgint2('1'), b: 2, count1: 2, count2: 2|>,
    <|a: pgint2('2'), b: 2, count1: 2, count2: 2|>,
];

$win_result = (
    SELECT
        COUNT(*) OVER w1 AS actual_count,
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
                a DESC
            RANGE BETWEEN UNBOUNDED PRECEDING AND pgnumeric('1') FOLLOWING
        ),
        w2 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a DESC
            RANGE BETWEEN UNBOUNDED PRECEDING AND pgfloat8('10000000000') FOLLOWING
        )
);

SELECT
    Ensure(actual_count, count1 IS NOT DISTINCT FROM actual_count),
    Ensure(actual_count2, count2 IS NOT DISTINCT FROM actual_count2),
FROM
    $win_result
;
