/* postgres can not */
SELECT
    AddTimezone(Datetime('2000-01-01T12:00:00Z'), 'Europe/Moscow'),
    AddTimezone(Datetime('2000-01-01T12:00:00Z'), 'America/Los_Angeles'),
    CAST(RemoveTimezone(TzDatetime('2000-01-01T12:00:00,Europe/Moscow')) AS string),
    CAST(RemoveTimezone(TzDatetime('2000-01-01T12:00:00,America/Los_Angeles')) AS string)
;
