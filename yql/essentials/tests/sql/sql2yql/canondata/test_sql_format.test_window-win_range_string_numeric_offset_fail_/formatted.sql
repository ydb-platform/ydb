PRAGMA WindowNewPipeline;

/* custom error: RANGE frames over an ORDER BY expression of type "String" support only RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW mode */
$data = [
    <|a: '1', count: 1|>,
    <|a: '2', count: 1|>,
    <|a: '3', count: 1|>,
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
        RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING
    )
;
