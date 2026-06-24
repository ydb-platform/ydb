PRAGMA WindowNewPipeline;

/* custom error: RANGE frames over an ORDER BY expression of type "Tuple<pgint4,pgint4>" support only RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW mode */
$data = [
    <|a: 1p, c: 1p, b: 1|>,
    <|a: 1p, c: 2p, b: 1|>,
    <|a: 2p, c: 1p, b: 1|>,
    <|a: 2p, c: 2p, b: 1|>,
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
