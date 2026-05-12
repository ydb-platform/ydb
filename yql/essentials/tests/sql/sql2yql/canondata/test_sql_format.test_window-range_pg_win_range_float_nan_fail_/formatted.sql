PRAGMA WindowNewPipeline;

/* custom error: invalid preceding or following size in window function */
$data = [
    <|a: pgfloat4('1.0'), count: 1|>,
    <|a: pgfloat4('2.0'), count: 1|>,
    <|a: pgfloat4('3.0'), count: 1|>,
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
        RANGE BETWEEN pgfloat8('NaN') PRECEDING AND pgfloat8('NaN') FOLLOWING
    )
;
