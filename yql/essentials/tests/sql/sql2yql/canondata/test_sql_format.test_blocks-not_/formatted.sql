$data = [<|x: TRUE|>, <|x: FALSE|>];

SELECT
    NOT x
FROM
    as_table($data)
;
