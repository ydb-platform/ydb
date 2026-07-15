$b = {4: 5};

SELECT
    DictLength(DictUpsert($b, a, 1))
FROM
    AS_TABLE(Opaque([<|a: 1|>, <|a: 2|>, <|a: 3|>]))
;
