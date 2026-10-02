PRAGMA WindowNewPipeline;

$data = [
    <|a: NULL, b: 1, count: 5|>,
    <|a: NULL, b: 1, count: 5|>,
    <|a: pgtimestamp('2017-11-27 13:22:00'), b: 1, count: 2|>,
    <|a: pgtimestamp('2017-11-27 13:23:00'), b: 1, count: 1|>,
    <|a: pgtimestamp('2017-11-27 13:24:00'), b: 1, count: 0|>,
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
            RANGE BETWEEN pginterval('1 minute') FOLLOWING AND UNBOUNDED FOLLOWING
        )
);

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
