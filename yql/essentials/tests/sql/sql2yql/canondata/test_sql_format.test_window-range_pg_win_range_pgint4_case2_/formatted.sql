PRAGMA WindowNewPipeline;

$data = [
    <|a: NULL, b: 1, sum11: NULL, count: 2|>,
    <|a: NULL, b: 1, sum11: NULL, count: 2|>,
    <|a: pgint4('100'), b: 1, sum11: pgint8('100'), count: 3|>,
    <|a: pgint4('200'), b: 1, sum11: pgint8('300'), count: 4|>,
    <|a: pgint4('250'), b: 1, sum11: pgint8('550'), count: 5|>,
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
            RANGE BETWEEN UNBOUNDED PRECEDING AND pgint4('10') FOLLOWING
        )
);

SELECT
    Ensure(actual_sum11, sum11 IS NOT DISTINCT FROM actual_sum11),
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
