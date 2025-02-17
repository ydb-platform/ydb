PRAGMA warning('disable', '4537');

$data = [<|x: 1|>, <|x: 2|>, <|x: 3|>];

SELECT
    *
FROM
    as_table($data)
LIMIT 2;

SELECT
    *
FROM
    as_table($data)
LIMIT 1 OFFSET 1;
