PRAGMA FeatureR010 = 'prototype';
PRAGMA config.flags('TimeOrderRecoverDelay', '-11');
PRAGMA config.flags('TimeOrderRecoverAhead', '11');
PRAGMA config.flags('MatchRecognizeStream', 'force');

$input = (
    SELECT
        *
    FROM
        AS_TABLE([
            <|time: 10, name: 'A'|>,
            <|time: 20, name: 'B'|>,
            <|time: 30, name: 'A'|>,
            <|time: 0, name: 'B'|>,
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
            LAST(B.time) AS b_time
        PATTERN (A B)
        DEFINE
            A AS A.name == 'A',
            B AS B.name == 'B'
    )
;
