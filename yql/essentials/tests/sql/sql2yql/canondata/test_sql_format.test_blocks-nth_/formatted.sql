$data = [<|x: (1, 'foo')|>, <|x: NULL|>];

SELECT
    x.1
FROM
    as_table($data)
;
