SELECT
    CAST(TzDate32('1900-01-01,Europe/Moscow') AS String),
    CAST('1900-01-01,Europe/Moscow' AS TzDate32),
    CAST(TzDatetime64('1900-01-01T01:02:03,Europe/Moscow') AS String),
    CAST('1900-01-01T01:02:03,Europe/Moscow' AS TzDatetime64),
    CAST(TzTimestamp64('1900-01-01T01:02:03.456789,Europe/Moscow') AS String),
    CAST('1900-01-01T01:02:03.456789,Europe/Moscow' AS TzTimestamp64),
    AddTimezone(Date32('1900-01-01'), 'Europe/Moscow'),
    AddTimezone(Datetime64('1900-01-01T01:02:03Z'), 'Europe/Moscow'),
    AddTimezone(Timestamp64('1900-01-01T01:02:03.456789Z'), 'Europe/Moscow'),
    CAST(RemoveTimezone(TzDate32('1900-01-02,Europe/Moscow')) AS String),
    CAST(RemoveTimezone(TzDatetime64('1900-01-01T03:32:20,Europe/Moscow')) AS String),
    CAST(RemoveTimezone(TzTimestamp64('1900-01-01T03:32:20.456789,Europe/Moscow')) AS String)
;
