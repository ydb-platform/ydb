$data = [<|x: <|a: 'foo'|>|>, <|x: <|a: NULL|>|>];

SELECT
    x.a
FROM
    as_table($data)
;
