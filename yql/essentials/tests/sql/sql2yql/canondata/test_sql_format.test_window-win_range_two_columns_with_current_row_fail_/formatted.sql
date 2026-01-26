PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

/* custom error: Range frame for multiple expressions is only allowed to be UNBOUNDED PRECEDING AND CURRENT ROW. */
$data = [
    <|a: 1, c: 1, b: 1|>,
    <|a: 1, c: 2, b: 1|>,
    <|a: 2, c: 1, b: 1|>,
    <|a: 2, c: 2, b: 1|>,
];

SELECT
    COUNT(*) OVER w1 AS cnt,
FROM
    AS_TABLE($data)
WINDOW
    w1 AS (
        PARTITION COMPACT BY
            b
        ORDER BY
            a ASC,
            c ASC
        RANGE BETWEEN CURRENT ROW AND CURRENT ROW
    )
;
