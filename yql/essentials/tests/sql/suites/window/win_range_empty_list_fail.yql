PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

/* custom error: Error while processing RANGE bound: Expected data or optional, but got EmptyList */
$data = [
    <|a: 1, count: 1|>,
    <|a: 2, count: 1|>,
    <|a: 3, count: 1|>,
];

SELECT
    COUNT(*) OVER w1 AS actual_count,
    count,
FROM
    AS_TABLE($data)
WINDOW
    w1 AS (
        ORDER BY
            a ASC
        RANGE BETWEEN [] PRECEDING AND 0 FOLLOWING
    )
;
