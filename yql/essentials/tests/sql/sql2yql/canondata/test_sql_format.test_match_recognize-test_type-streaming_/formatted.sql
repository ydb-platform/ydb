PRAGMA FeatureR010 = 'prototype';
PRAGMA config.flags('MatchRecognizeStream', 'force');

$data = [<|dt: 4, host: 'fqdn1', key: 14|>];

-- NoPartitionNoMeasure
SELECT
    *
FROM
    AS_TABLE($data) MATCH_RECOGNIZE (
        ORDER BY
            CAST(dt AS Timestamp)
        ONE ROW PER MATCH
        AFTER MATCH SKIP TO NEXT ROW
        PATTERN (Y)
        DEFINE
            Y AS NULL
    )
;

--NoPartitionStringMeasure
SELECT
    *
FROM
    AS_TABLE($data) MATCH_RECOGNIZE (
        ORDER BY
            CAST(dt AS Timestamp)
        MEASURES
            'SomeString' AS Measure1
        ONE ROW PER MATCH
        AFTER MATCH SKIP TO NEXT ROW
        PATTERN (Q)
        DEFINE
            Q AS TRUE
    )
;

--IntPartitionColNoMeasure
SELECT
    *
FROM
    AS_TABLE($data) MATCH_RECOGNIZE (
        PARTITION BY
            key
        ORDER BY
            CAST(dt AS Timestamp)
        ONE ROW PER MATCH
        AFTER MATCH SKIP TO NEXT ROW
        PATTERN (L)
        DEFINE
            L AS JUST(TRUE)
    )
;

--StringPartitionColStringMeasure
SELECT
    *
FROM
    AS_TABLE($data) MATCH_RECOGNIZE (
        PARTITION BY
            host
        ORDER BY
            CAST(dt AS Timestamp)
        MEASURES
            'SomeString' AS Measure1
        ONE ROW PER MATCH
        AFTER MATCH SKIP TO NEXT ROW
        PATTERN (Y)
        DEFINE
            Y AS TRUE
    )
;

--TwoPartitionColsTwoMeasures
SELECT
    *
FROM
    AS_TABLE($data) MATCH_RECOGNIZE (
        PARTITION BY
            host,
            key
        ORDER BY
            CAST(dt AS Timestamp)
        MEASURES
            'SomeString' AS S,
            345 AS I
        ONE ROW PER MATCH
        AFTER MATCH SKIP TO NEXT ROW
        PATTERN (Q)
        DEFINE
            Q AS TRUE
    )
;
