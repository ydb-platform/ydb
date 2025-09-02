PRAGMA config.flags('OptimizerFlags', 'EqualityFilterOverJoin');

$p = 1;

$simpleKey = (
    SELECT
        *
    FROM
        as_table([<|Key: Just(1), Value: 'qqq'|>, <|Key: Just(2), Value: 'aaa'|>])
);

$complexKey = (
    SELECT
        *
    FROM
        as_table([<|Key: Just(2), Fk: 2, Value: 'zzz'|>, <|Key: Just(2), Fk: 3, Value: 'ttt'|>])
);

SELECT
    l.Key,
    l.Fk,
    l.Value,
    r.Key,
    r.Value
FROM
    $simpleKey AS r
INNER JOIN
    $complexKey AS l
ON
    l.Fk == r.Key
WHERE
    l.Key == 1 + $p AND l.Key == l.Key
ORDER BY
    r.Value
;
