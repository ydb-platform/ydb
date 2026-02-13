PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

/* custom error: Error while processing RANGE bound: NaN is not allowed for RANGE frame bounds */
$data = [
    <|a: Float('1.0'), count: 1|>,
    <|a: Float('2.0'), count: 1|>,
    <|a: Float('3.0'), count: 1|>,
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
        RANGE BETWEEN Float('nan') PRECEDING AND Float('nan') FOLLOWING
    )
;
