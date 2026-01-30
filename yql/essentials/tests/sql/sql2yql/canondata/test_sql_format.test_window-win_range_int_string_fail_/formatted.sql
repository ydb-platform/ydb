PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

/* custom error: Error while processing RANGE bound: Cannot add type Optional<Int32> and String */
$data = [
    <|a: NULL, count: 1|>,
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
        RANGE BETWEEN '1' PRECEDING AND '1' FOLLOWING
    )
;
