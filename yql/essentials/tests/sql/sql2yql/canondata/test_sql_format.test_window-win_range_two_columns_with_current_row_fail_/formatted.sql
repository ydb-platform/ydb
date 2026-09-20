PRAGMA WindowNewPipeline;

/* custom error: RANGE frames over an ORDER BY expression of type "Tuple<Int32,Int32>" support only RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW mode */
$data = [
    <|a: 1, c: 1, b: 1|>,
    <|a: 1, c: 2, b: 1|>,
    <|a: 2, c: 1, b: 1|>,
    <|a: 2, c: 2, b: 1|>,
];

SELECT
    COUNT(*) OVER w1 AS cnt,
FROM
    AS_TABLE($data)
WINDOW
    w1 AS (
        PARTITION COMPACT BY
            b
        ORDER BY
            a ASC,
            c ASC
        RANGE BETWEEN CURRENT ROW AND CURRENT ROW
    )
;
