PRAGMA FeatureR010 = 'prototype';

$input = (
    SELECT
        *
    FROM
        AS_TABLE([
            <|time: 0, value: 0|>,
            <|time: 100, value: 1|>,
            <|time: 200, value: 2|>,
            <|time: 300, value: 3|>,
            <|time: 400, value: 4|>,
            <|time: 500, value: 5|>,
            <|time: 600, value: 0|>,
            <|time: 700, value: 1|>,
            <|time: 800, value: 2|>,
            <|time: 900, value: 3|>,
            <|time: 1000, value: 4|>,
            <|time: 1100, value: 5|>,
            <|time: 1200, value: 0|>,
        ])
);

SELECT
    *
FROM
    $input MATCH_RECOGNIZE (
        ORDER BY
            CAST(time AS Timestamp)
        MEASURES
            FIRST(A.time) AS a_time,
            FIRST(B.time) AS b_time,
            LAST(C.time) AS c_time,
            LAST(F.time) AS f_time
        ALL ROWS PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (A B {- C -} D {- E -} F +)
        DEFINE
            A AS A.value == 0
            AND COALESCE(A.time - FIRST(A.time) <= 1000, TRUE),
            B AS B.value == 1
            AND COALESCE(B.time - FIRST(A.time) <= 1000, TRUE),
            C AS C.value == 2
            AND COALESCE(C.time - FIRST(A.time) <= 1000, TRUE),
            D AS D.value == 3
            AND COALESCE(D.time - FIRST(A.time) <= 1000, TRUE),
            E AS E.value == 4
            AND COALESCE(E.time - FIRST(A.time) <= 1000, TRUE),
            F AS F.value == 5
            AND COALESCE(F.time - FIRST(A.time) <= 1000, TRUE)
    )
;
