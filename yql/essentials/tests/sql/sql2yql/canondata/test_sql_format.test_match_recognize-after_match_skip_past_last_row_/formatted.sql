PRAGMA FeatureR010 = 'prototype';
PRAGMA config.flags('MatchRecognizeStream', 'disable');

$input = (
    SELECT
        *
    FROM
        AS_TABLE([
            <|time: 0|>,
            <|time: 1|>,
            <|time: 2|>,
            <|time: 3|>,
        ])
);

SELECT
    *
FROM
    $input MATCH_RECOGNIZE (
        ORDER BY
            CAST(time AS Timestamp)
        MEASURES
            FIRST(X.time) AS first_time,
            LAST(X.time) AS last_time
        PATTERN (X {2})
        DEFINE
            X AS TRUE
    )
;
