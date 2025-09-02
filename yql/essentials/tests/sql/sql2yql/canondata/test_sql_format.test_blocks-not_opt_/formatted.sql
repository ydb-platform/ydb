$data = [<|x: TRUE|>, <|x: FALSE|>, <|x: NULL|>];

SELECT
    NOT x
FROM
    as_table($data)
;
