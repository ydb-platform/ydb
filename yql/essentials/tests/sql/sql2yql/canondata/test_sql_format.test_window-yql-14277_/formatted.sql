/* syntax version 1 */
$data = [
    <|id: 1, time: 1, value: 'a'|>,
    <|id: 1, time: 2, value: NULL|>,
    <|id: 1, time: 3, value: NULL|>,
    <|id: 1, time: 4, value: 'b'|>,
    <|id: 1, time: 5, value: NULL|>,
    <|id: 2, time: 1, value: 'c'|>,
    <|id: 2, time: 2, value: 'd'|>,
    <|id: 2, time: 3, value: NULL|>,
];

SELECT
    a.*,
    count(value) OVER w1 AS w1,
    max(value) OVER w2 AS w2,
FROM
    as_table($data) AS a
WINDOW
    w1 AS (
        ORDER BY
            time,
            id
    ),
    w2 AS (
        PARTITION BY
            id
    )
ORDER BY
    id,
    time
;
