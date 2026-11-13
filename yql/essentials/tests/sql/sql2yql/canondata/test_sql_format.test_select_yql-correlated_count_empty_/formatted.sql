-- YQL-21769: COUNT(*) must return zero for an empty correlated group.
PRAGMA YqlSelect = 'force';

SELECT
    COUNT(*)
FROM
    AS_TABLE([<|a: 0|>, <|a: 1|>, <|a: 2|>]) AS A
WHERE
    0 == (
        SELECT
            COUNT(*)
        FROM
            AS_TABLE([<|b: 1|>, <|b: 2|>, <|b: 3|>]) AS B
        WHERE
            A.a == B.b
    )
;
