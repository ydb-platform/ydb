PRAGMA WindowNewPipeline;

/* custom error: RANGE frames over an ORDER BY expression of type "String" support only RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW mode */
$data = [
    <|a: 'apple', b: 1, count: 1|>,
    <|a: 'banana', b: 1, count: 2|>,
];

SELECT
    COUNT(*) OVER w1 AS actual_count,
    count,
FROM
    AS_TABLE($data)
WINDOW
    w1 AS (
        PARTITION COMPACT BY
            b
        ORDER BY
            a ASC
        RANGE BETWEEN CURRENT ROW AND CURRENT ROW
    )
;
