PRAGMA WindowNewPipeline;

$data = [
    <|a: pgtimestamptz('2017-11-27 13:24:00.123454+00'), b: 1, count: 2|>,
    <|a: pgtimestamptz('2017-11-27 13:24:00.123455+00'), b: 1, count: 1|>,
    <|a: pgtimestamptz('2017-11-27 13:24:00.123456+00'), b: 1, count: 0|>,
    <|a: NULL, b: 1, count: 1|>,
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
            RANGE BETWEEN pginterval('0.000001 seconds') FOLLOWING AND pginterval('0.000003 seconds') FOLLOWING
        )
);

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
