/* postgres can not */
SELECT
    CAST(CAST(TzDate('2000-01-01,Europe/Moscow') AS string) AS TzDate),
    CAST(CAST(TzDatetime('2000-01-01T01:02:03,Europe/Moscow') AS string) AS TzDatetime),
    CAST(CAST(TzTimestamp('2000-01-01T01:02:03.456789,Europe/Moscow') AS string) AS TzTimestamp)
;
