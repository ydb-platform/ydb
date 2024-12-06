pragma FeatureR010="prototype";
pragma config.flags("MatchRecognizeStream", "disable");

$input = SELECT * FROM AS_TABLE([
    <|time: 0,    id: 1, name: 'A'|>,
    <|time: 200,  id: 2, name: 'B'|>,
    <|time: 400,  id: 3, name: 'C'|>,
    <|time: 600,  id: 4, name: 'B'|>,
    <|time: 800,  id: 5, name: 'C'|>,
    <|time: 1000, id: 6, name: 'W'|>,
]);


SELECT * FROM $input MATCH_RECOGNIZE (
    ORDER BY CAST(time AS Timestamp)
    MEASURES
        FIRST(A.id) as a_id,
        LAST(B_OR_C.id) as bc_id,
        LAST(C.id) as c_id
    PATTERN (A B_OR_C* C)
    DEFINE
        A  AS  A.name ='A',
        B_OR_C AS (B_OR_C.name ='B' or B_OR_C.name ='C'),
        C  AS  C.name ='C'
    );

