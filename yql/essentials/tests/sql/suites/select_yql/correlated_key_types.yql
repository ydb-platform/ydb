PRAGMA YqlSelect = 'force';

SELECT
    A.a,
    A.b,
    (
        SELECT
            COUNT(*)
        FROM
            AS_TABLE([<|a: 0, b: 1|>, <|a: 1, b: 1|>]) AS B
        WHERE
            A.a == B.a AND (A.b == B.b OR A.b IS NULL)
    ) AS count
FROM
    AS_TABLE([<|a: 0, b: Just(1)|>, <|a: 1, b: Nothing(Int32?)|>, <|a: 2, b: Just(1)|>]) AS A
ORDER BY
    A.a
;

SELECT
    A.a,
    (
        SELECT
            COUNT(*)
        FROM
            AS_TABLE([<|b: (0, 1)|>, <|b: (1, 2)|>]) AS B
        WHERE
            A.a == B.b
    ) AS count
FROM
    AS_TABLE([<|a: (0, 1)|>, <|a: (2, 3)|>]) AS A
ORDER BY
    A.a
;

SELECT
    A.a,
    (
        SELECT
            COUNT(*)
        FROM
            AS_TABLE([<|b: 0|>, <|b: 1|>]) AS B
        WHERE
            A.a == B.b
    ) AS count
FROM
    AS_TABLE([<|a: 0|>, <|a: 2|>]) AS A
ORDER BY
    A.a
;

SELECT
    A.a,
    (
        SELECT
            COUNT(*)
        FROM
            AS_TABLE([<|b: 0|>, <|b: 1|>]) AS B
        WHERE
            A.a == B.b OR A.a IS NULL
    ) AS count
FROM
    AS_TABLE([<|a: Just(0)|>, <|a: Nothing(Int32?)|>, <|a: Just(2)|>]) AS A
ORDER BY
    A.a
;

SELECT
    A.a,
    (
        SELECT
            COUNT(*)
        FROM
            AS_TABLE([<|b: 0|>, <|b: 1|>]) AS B
        WHERE
            A.a IS NULL AND B.b == 0
    ) AS count
FROM
    AS_TABLE([<|a: NULL|>]) AS A
;

SELECT
    A.a,
    (
        SELECT
            COUNT(*)
        FROM
            AS_TABLE([<|b: (0, Just(1))|>, <|b: (0, Nothing(Int32?))|>]) AS B
        WHERE
            A.a IS NOT DISTINCT FROM B.b
    ) AS count
FROM
    AS_TABLE([<|a: (0, Just(1))|>, <|a: (0, Nothing(Int32?))|>, <|a: (2, Just(1))|>]) AS A
ORDER BY
    A.a
;
