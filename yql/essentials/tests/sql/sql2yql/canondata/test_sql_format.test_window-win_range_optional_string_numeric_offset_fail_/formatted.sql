PRAGMA WindowNewPipeline;

/* custom error: RANGE frames over an ORDER BY expression of type "Optional<String>" support only RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW mode */
$data = [
    <|a: just('1'), count: 1|>,
    <|a: just('2'), count: 1|>,
    <|a: NULL, count: 1|>,
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
