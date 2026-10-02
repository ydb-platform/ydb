PRAGMA WindowNewPipeline;

$data = [
    <|a: NULL, b: 1, sum11: NULL, count: 0|>,
    <|a: NULL, b: 1, sum11: NULL, count: 0|>,
    <|a: pgint2('8'), b: 1, sum11: NULL, count: 0|>,
    <|a: pgint2('10'), b: 1, sum11: NULL, count: 0|>,
    <|a: pgint2('11'), b: 1, sum11: NULL, count: 0|>,
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
            RANGE BETWEEN pgint2('1') PRECEDING AND pgint2('2') PRECEDING
        )
);

SELECT
    Ensure(actual_sum11, Nothing(pgint8) IS NOT DISTINCT FROM actual_sum11),
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
