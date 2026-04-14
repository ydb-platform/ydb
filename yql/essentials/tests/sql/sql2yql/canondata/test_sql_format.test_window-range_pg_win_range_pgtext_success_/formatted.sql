PRAGMA WindowNewPipeline;

$data = [
    <|a: NULL, b: 1, count: 2|>,
    <|a: NULL, b: 1, count: 2|>,
    <|a: pgtext('apple'), b: 1, count: 3|>,
    <|a: pgtext('banana'), b: 1, count: 4|>,
    <|a: pgtext('cherry'), b: 1, count: 6|>,
    <|a: pgtext('cherry'), b: 1, count: 6|>,
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
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
);

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
