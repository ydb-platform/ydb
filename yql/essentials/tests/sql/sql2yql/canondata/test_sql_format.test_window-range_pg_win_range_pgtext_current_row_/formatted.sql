PRAGMA WindowNewPipeline;

$data = [
    <|a: pgtext('apple'), b: 1, count: 1|>,
    <|a: pgtext('banana'), b: 1, count: 2|>,
];

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
        RANGE BETWEEN CURRENT ROW AND CURRENT ROW
    )
;
