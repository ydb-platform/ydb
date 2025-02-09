PRAGMA config.flags('PeepholeFlags', 'UseAggPhases');

$data = [<|x: 1|>, <|x: 3|>, <|x: 2|>];

SELECT
    sum(x)
FROM
    as_table($data)
;
