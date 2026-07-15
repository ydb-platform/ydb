PRAGMA WindowNewPipeline;

$data = [
    <|a: pgint4('-50000'), b: 1, sum11: pgint8('-50000'), count1: 1, sum22: NULL, count2: 0|>,
    <|a: pgint4('-10000'), b: 1, sum11: pgint8('-60000'), count1: 2, sum22: pgint8('-50000'), count2: 1|>,
    <|a: pgint4('0'), b: 1, sum11: pgint8('-60000'), count1: 3, sum22: pgint8('-10000'), count2: 1|>,
    <|a: NULL, b: 1, sum11: NULL, count1: 2, sum22: NULL, count2: 2|>,
    <|a: NULL, b: 1, sum11: NULL, count1: 2, sum22: NULL, count2: 2|>,
];

$win_result = (
    SELECT
        pg::sum(a) OVER w1 AS actual_sum11,
        COUNT(*) OVER w1 AS actual_count1,
        pg::sum(a) OVER w2 AS actual_sum22,
        COUNT(*) OVER w2 AS actual_count2,
        sum11,
        count1,
        sum22,
        count2,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN pgint4('50000') PRECEDING AND 0p PRECEDING
        ),
        w2 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN pgint4('40000') PRECEDING AND pgint4('10000') PRECEDING
        )
);

SELECT
    Ensure(actual_sum11, sum11 IS NOT DISTINCT FROM actual_sum11),
    Ensure(actual_count1, count1 IS NOT DISTINCT FROM actual_count1),
    Ensure(actual_sum22, sum22 IS NOT DISTINCT FROM actual_sum22),
    Ensure(actual_count2, count2 IS NOT DISTINCT FROM actual_count2)
FROM
    $win_result
;
