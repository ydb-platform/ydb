PRAGMA YqlSelect = 'force';

$a = 'a';

SELECT
    v
FROM
    AS_TABLE([<|k: 0|>]) AS x
CROSS JOIN
    AS_TABLE([<|x: <|a: [1]|>, a: <|a: [2]|>|>]) AS s
    FLATTEN LIST BY (
        x.$a AS v
    )
;
