$data = [<|x: 1|>, <|x: 2|>];

SELECT
    *
FROM (
    SELECT
        x,
        [3, 4] AS y
    FROM
        as_table($data)
)
    FLATTEN LIST BY y
;
