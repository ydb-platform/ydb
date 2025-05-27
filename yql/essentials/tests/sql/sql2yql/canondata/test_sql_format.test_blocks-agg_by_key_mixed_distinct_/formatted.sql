PRAGMA config.flags('PeepholeFlags', 'UseAggPhases');

$data = [<|x: 1, y: 0|>, <|x: 1, y: 0|>, <|x: 2, y: 1|>];

SELECT
    y,
    min(x),
    sum(DISTINCT x)
FROM
    as_table($data)
GROUP BY
    y
ORDER BY
    y
;
