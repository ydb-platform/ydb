PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

/* custom error: Error while processing RANGE bound: Cannot add type TzDate and Int64 */
$data = [
    <|a: TzDate('2024-01-01,Europe/Moscow'), count: 1|>,
    <|a: TzDate('2024-01-02,Europe/Moscow'), count: 1|>,
    <|a: TzDate('2024-01-03,Europe/Moscow'), count: 1|>,
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
        RANGE BETWEEN Int64('86400000000') PRECEDING AND Int64('86400000000') FOLLOWING
    )
;
