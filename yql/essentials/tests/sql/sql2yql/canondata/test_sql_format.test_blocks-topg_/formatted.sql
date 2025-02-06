$data = [<|x: 1|>, <|x: NULL|>];

SELECT
    ToPg(x)
FROM
    as_table($data)
;
