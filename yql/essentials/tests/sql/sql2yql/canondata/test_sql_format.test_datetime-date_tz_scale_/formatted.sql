/* postgres can not */
SELECT
    CAST(TzDate('2000-01-01,Europe/Moscow') AS TzDatetime)
;

SELECT
    CAST(TzDate('2000-01-01,Europe/Moscow') AS TzTimestamp)
;

SELECT
    CAST(TzDatetime('2000-01-01T12:00:00,Europe/Moscow') AS TzDate)
;

SELECT
    CAST(TzDatetime('2000-01-01T12:00:00,Europe/Moscow') AS TzTimestamp)
;

SELECT
    CAST(TzTimestamp('2000-01-01T12:00:00.456789,Europe/Moscow') AS TzDate)
;

SELECT
    CAST(TzTimestamp('2000-01-01T12:00:00.456789,Europe/Moscow') AS TzDatetime)
;

SELECT
    CAST(TzDate('2000-01-01,America/Los_Angeles') AS TzDatetime)
;

SELECT
    CAST(TzDate('2000-01-01,America/Los_Angeles') AS TzTimestamp)
;

SELECT
    CAST(TzDatetime('2000-01-01T12:00:00,America/Los_Angeles') AS TzDate)
;

SELECT
    CAST(TzDatetime('2000-01-01T12:00:00,America/Los_Angeles') AS TzTimestamp)
;

SELECT
    CAST(TzTimestamp('2000-01-01T12:00:00.456789,America/Los_Angeles') AS TzDate)
;

SELECT
    CAST(TzTimestamp('2000-01-01T12:00:00.456789,America/Los_Angeles') AS TzDatetime)
;
