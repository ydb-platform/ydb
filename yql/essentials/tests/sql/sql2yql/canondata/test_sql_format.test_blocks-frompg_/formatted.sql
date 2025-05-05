$data = [<|x: 1p|>, <|x: NULL|>];

SELECT
    FromPg(x)
FROM
    as_table($data)
;
