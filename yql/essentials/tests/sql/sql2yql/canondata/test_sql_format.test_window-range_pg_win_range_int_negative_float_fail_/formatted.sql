PRAGMA WindowNewPipeline;

/* custom error: Expected positive literal value */
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
        RANGE BETWEEN -1p PRECEDING AND 1p FOLLOWING
    )
;
