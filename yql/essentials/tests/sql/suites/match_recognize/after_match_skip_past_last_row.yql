pragma FeatureR010="prototype";
pragma config.flags("MatchRecognizeStream", "disable");

$input = SELECT * FROM AS_TABLE([
    <|time:0|>,
    <|time:1|>,
    <|time:2|>,
    <|time:3|>,
]);

SELECT * FROM $input MATCH_RECOGNIZE(
    ORDER BY CAST(time as Timestamp)
    MEASURES
        FIRST(X.time) as first_time,
        LAST(X.time) as last_time
    PATTERN (X{2})
    DEFINE
        X as True
);
