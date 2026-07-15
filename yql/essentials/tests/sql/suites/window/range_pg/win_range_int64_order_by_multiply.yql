PRAGMA WindowNewPipeline;

$data = [
    <|a: pgint8('-1'), b: 1, sum11: pgnumeric('-3'), count: 2|>,
    <|a: pgint8('-2'), b: 1, sum11: pgnumeric('-6'), count: 3|>,
    <|a: pgint8('-3'), b: 1, sum11: pgnumeric('-5'), count: 2|>,
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
            PARTITION BY
                b
            ORDER BY
                a * 5p ASC
            RANGE BETWEEN pgint8('5') PRECEDING AND pgint8('5') FOLLOWING
        )
);

SELECT
    Ensure(actual_sum11, sum11 IS NOT DISTINCT FROM actual_sum11),
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
