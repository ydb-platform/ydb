PRAGMA WindowNewPipeline;

$data = [
    <|a: pgtimestamp('2017-11-27 13:24:00.123454'), b: 1, count: 5|>,
    <|a: pgtimestamp('2017-11-27 13:24:00.123455'), b: 1, count: 5|>,
    <|a: pgtimestamp('2017-11-27 13:24:00.123456'), b: 1, count: 5|>,
    <|a: NULL, b: 1, count: 5|>,
    <|a: NULL, b: 1, count: 5|>,
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
            RANGE BETWEEN CURRENT ROW AND CURRENT ROW
        )
);

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
