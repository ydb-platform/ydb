USE plato;
$min_ts_for_stat_calculation = DateTime::ToSeconds(CurrentUtcDate() - Interval("P1D"));

INSERT INTO @a
SELECT
    *
FROM (
    SELECT
        1ul AS puid,
        CurrentUtcTimestamp() AS timestamp,
        [1, 2] AS segments,
        "a" AS dummy1
)
ASSUME ORDER BY
    puid,
    timestamp DESC;

INSERT INTO @b
SELECT
    *
FROM (
    SELECT
        4ul AS puid,
        CurrentUtcTimestamp() AS timestamp,
        [3, 2] AS segments,
        "a" AS dummy1
)
ASSUME ORDER BY
    puid,
    timestamp DESC;

INSERT INTO @c
SELECT
    *
FROM (
    SELECT
        2ul AS puid,
        Just(CurrentUtcTimestamp()) AS timestamp,
        [2, 3] AS segments,
        "a" AS dummy2
)
ASSUME ORDER BY
    puid,
    timestamp DESC;
COMMIT;

$target_events = (
    SELECT
        puid,
        segments
    FROM CONCAT(@a, @b, @c)
    WHERE DateTime::ToSeconds(`timestamp`) > $min_ts_for_stat_calculation
);

$target_events = (
    SELECT DISTINCT
        *
    FROM (
        SELECT
            *
        FROM $target_events
            FLATTEN LIST BY segments
    )
        FLATTEN COLUMNS
);

SELECT
    *
FROM $target_events
ORDER BY
    puid,
    segments;
