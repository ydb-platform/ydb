PRAGMA WindowNewPipeline;

$data = [
    <|a: pgtimestamptz('2017-11-27 13:22:00+00'), b: 1, count: 0|>,
    <|a: pgtimestamptz('2017-11-27 13:23:00+00'), b: 1, count: 1|>,
    <|a: pgtimestamptz('2017-11-27 13:24:00+00'), b: 1, count: 2|>,
    <|a: pgtimestamptz('2017-11-27 13:22:00+00'), b: 2, count: 0|>,
    <|a: pgtimestamptz('2017-11-27 13:23:00+00'), b: 2, count: 1|>,
    <|a: pgtimestamptz('2017-11-27 13:24:00+00'), b: 3, count: 0|>,
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
            RANGE BETWEEN pginterval('3 minutes') PRECEDING AND pginterval('1 minute') PRECEDING
        )
);

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
