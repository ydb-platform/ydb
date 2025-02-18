PRAGMA config.flags('PeepholeFlags', 'UseAggPhases');

$data = [<|x: 1|>, <|x: 1|>, <|x: 2|>];

SELECT
    min(x),
    sum(DISTINCT x)
FROM
    as_table($data)
;
