$data1 = [<|x: 1|>, <|x: 3|>, <|x: 2|>];
$data2 = [<|x: 4|>, <|x: 1|>];

SELECT
    *
FROM
    as_table($data1)
UNION ALL
SELECT
    *
FROM
    as_table($data2)
;
