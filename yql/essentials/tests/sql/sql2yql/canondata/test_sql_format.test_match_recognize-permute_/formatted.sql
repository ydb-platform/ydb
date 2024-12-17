$data = [
    <|dt: 1688910000, event: 'A'|>,
    <|dt: 1688910100, event: 'B'|>,
    <|dt: 1688910200, event: 'C'|>,
    <|dt: 1688910300, event: 'A'|>,
    <|dt: 1688910400, event: 'C'|>,
    <|dt: 1688910500, event: 'D'|>,
    <|dt: 1688910500, event: 'C'|>,
    <|dt: 1688910600, event: 'B'|>,
    <|dt: 1688910800, event: 'A'|>,
    <|dt: 1688910900, event: 'C'|>,
    <|dt: 1688911000, event: 'B'|>,
];

PRAGMA FeatureR010 = 'prototype';

SELECT
    *
FROM
    AS_TABLE($data) MATCH_RECOGNIZE (
        ORDER BY
            CAST(dt AS Timestamp)
        MEASURES
            FIRST(A.dt) AS a,
            FIRST(B.dt) AS b,
            FIRST(C.dt) AS c
        ONE ROW PER MATCH
        AFTER MATCH SKIP TO NEXT ROW
        PATTERN (PERMUTE (A, B, C))
        DEFINE
            A AS A.event == 'A',
            B AS B.event == 'B',
            C AS C.event == 'C'
    ) AS MATCHED
;
