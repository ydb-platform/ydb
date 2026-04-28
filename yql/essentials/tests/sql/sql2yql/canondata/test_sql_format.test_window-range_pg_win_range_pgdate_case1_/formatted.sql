PRAGMA WindowNewPipeline;

$data = [
    <|a: pgdate('2017-11-24'), b: 1, count: 0|>,
    <|a: pgdate('2017-11-25'), b: 1, count: 1|>,
    <|a: pgdate('2017-11-26'), b: 1, count: 2|>,
    <|a: pgdate('2017-11-27'), b: 1, count: 3|>,
    <|a: NULL, b: 1, count: 2|>,
    <|a: NULL, b: 1, count: 2|>,
];

$win_result = (
    SELECT
        COUNT(*) OVER w1 AS actual_count,
        count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN pginterval('3 days') PRECEDING AND pginterval('1 day') PRECEDING
        )
);

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
