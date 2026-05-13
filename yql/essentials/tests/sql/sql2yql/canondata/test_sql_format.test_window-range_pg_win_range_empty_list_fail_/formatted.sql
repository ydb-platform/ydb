PRAGMA WindowNewPipeline;

/* custom error: Expected literal of pg type */
$data = [
    <|a: 1p, count: 1|>,
    <|a: 2p, count: 1|>,
    <|a: 3p, count: 1|>,
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
        RANGE BETWEEN [] PRECEDING AND 0p FOLLOWING
    )
;
