$data = [<|x: 1, y: 'foo'|>];

SELECT
    (x, 1),
    (x, y)
FROM
    as_table($data)
;
