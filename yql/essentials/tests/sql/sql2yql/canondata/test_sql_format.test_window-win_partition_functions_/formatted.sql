$data = [
    <|a: 1, b: 1, count: 1|>,
    <|a: 2, b: 1, count: 2|>,
    <|a: 2, b: 1, count: 2|>,
    <|a: NULL, b: 1, count: 2|>,
    <|a: NULL, b: 1, count: 2|>,
];

SELECT
    COUNT(*) OVER w1,
    RANK() OVER w1,
    DENSE_RANK() OVER w1,
    ROW_NUMBER() OVER w1,
    NTILE(3) OVER w1,
    PERCENT_RANK() OVER w1,
FROM
    AS_TABLE($data)
WINDOW
    w1 AS (
        PARTITION COMPACT BY
            b
        ORDER BY
            a ASC
    )
;

SELECT
    CUME_DIST() OVER w1,
FROM
    AS_TABLE($data)
WINDOW
    w1 AS (
        PARTITION COMPACT BY
            b
        ORDER BY
            a ASC
    )
;
