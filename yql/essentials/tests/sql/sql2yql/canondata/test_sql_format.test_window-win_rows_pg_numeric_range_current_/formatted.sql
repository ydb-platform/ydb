$data = [
    <|a: NULL, b: 1, sum11: NULL, count: 2|>,
    <|a: NULL, b: 1, sum11: NULL, count: 2|>,
    <|a: pgnumeric('1000000'), b: 1, sum11: pgnumeric('6000000'), count: 3|>,
    <|a: pgnumeric('2000000'), b: 1, sum11: pgnumeric('6000000'), count: 3|>,
    <|a: pgnumeric('3000000'), b: 1, sum11: pgnumeric('6000000'), count: 3|>,
];

SELECT
    pg::sum(a) OVER w1 AS actual_sum11,
    COUNT(*) OVER w1 AS actual_count,
    sum11,
    count,
FROM
    AS_TABLE($data)
WINDOW
    w1 AS (
        PARTITION COMPACT BY
            b
        ORDER BY
            a ASC
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
;
