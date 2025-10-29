PRAGMA FeatureR010="prototype";
PRAGMA config.flags("MatchRecognizeStream", "disable");

$input = SELECT * FROM AS_TABLE([
    <|time: 0,   name: 'A'|>,
    <|time: 100, name: 'B'|>,
    <|time: 200, name: 'C'|>,
    <|time: 300, name: 'B'|>,
    <|time: 400, name: 'C'|>,
    <|time: 500, name: 'A'|>,
    <|time: 600, name: 'B'|>,
    <|time: 700, name: 'C'|>,
    <|time: 800, name: 'W'|>,
]);

SELECT * FROM $input MATCH_RECOGNIZE (
    ORDER BY CAST(time AS Timestamp)
    MEASURES
        FIRST(A.time) AS a_time,
        LAST(B_OR_C.time) AS bc_time,
        LAST(C.time) AS c_time
    PATTERN (A B_OR_C* C)
    DEFINE
        A AS A.name = 'A',
        B_OR_C AS (B_OR_C.name = 'B' OR B_OR_C.name = 'C'),
        C AS C.name = 'C'
);
