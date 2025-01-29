$data = [<|x: nothing(int32?)|>, <|x: just(1)|>];

SELECT
    x IS NOT NULL
FROM
    as_table($data)
;
