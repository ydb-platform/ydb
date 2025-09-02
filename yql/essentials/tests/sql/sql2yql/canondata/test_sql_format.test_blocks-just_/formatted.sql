$data = [<|x: 1|>];

SELECT
    just(x)
FROM
    as_table($data)
;
