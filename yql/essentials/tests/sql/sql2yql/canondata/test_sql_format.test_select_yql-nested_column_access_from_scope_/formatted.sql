PRAGMA YqlSelect = 'force';

SELECT
    v
FROM
    AS_TABLE([<|k: 0|>]) AS x
CROSS JOIN (
    SELECT
        x.a AS v
    FROM
        AS_TABLE([<|x: <|a: 1|>|>])
) AS q;
