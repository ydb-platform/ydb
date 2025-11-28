PRAGMA config.flags('OptimizerFlags', 'NormalizeEqualityFilterOverJoin');

$left = [<|x: Just(1), y: 2|>, <|x: 2, y: 3|>, <|x: 3, y: 4|>];
$right = [<|a: 2, b: 3|>, <|a: 3, b: 4|>, <|a: 4, b: 5|>];

SELECT
    *
FROM (
    SELECT
        *
    FROM
        as_table($left) AS l
    LEFT JOIN
        as_table($right) AS r
    ON
        l.y == r.b
)
WHERE
    ((x > 0) ?? FALSE) == ((a > 0) ?? FALSE)
;
