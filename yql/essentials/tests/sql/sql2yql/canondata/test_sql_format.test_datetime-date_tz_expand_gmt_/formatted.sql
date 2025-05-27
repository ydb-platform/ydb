/* postgres can not */
SELECT
    CAST(Date('2000-01-01') AS TzDate),
    CAST(Datetime('2000-01-01T01:02:03Z') AS TzDatetime),
    CAST(Timestamp('2000-01-01T01:02:03.456789Z') AS TzTimestamp)
;
