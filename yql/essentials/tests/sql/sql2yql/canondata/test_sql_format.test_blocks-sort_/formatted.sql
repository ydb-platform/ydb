$data = [<|x: 1|>, <|x: 3|>, <|x: 2|>];

SELECT
    *
FROM
    as_table($data)
ORDER BY
    x
;

SELECT
    *
FROM
    as_table($data)
ORDER BY
    x DESC
LIMIT 2;
