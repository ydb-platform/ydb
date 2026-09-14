PRAGMA WindowNewPipeline;

$data = [
    <|a: NULL, b: 1, sum11: NULL, count: 2|>,
    <|a: NULL, b: 1, sum11: NULL, count: 2|>,
    <|a: pgint2('8'), b: 1, sum11: pgint8('8'), count: 1|>,
    <|a: pgint2('10'), b: 1, sum11: pgint8('10'), count: 1|>,
    <|a: pgint2('11'), b: 1, sum11: pgint8('21'), count: 2|>,
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
            RANGE BETWEEN pgint2('1') PRECEDING AND CURRENT ROW
        )
);

SELECT
    Ensure(actual_sum11, sum11 IS NOT DISTINCT FROM actual_sum11),
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
