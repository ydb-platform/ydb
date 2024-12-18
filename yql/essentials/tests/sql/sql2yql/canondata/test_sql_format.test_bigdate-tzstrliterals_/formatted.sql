SELECT
    CAST(Date32('-144169-01-01') AS string),
    TzDate32('-144169-01-01,UTC'),
    TzDate32('-144169-01-02,Europe/Moscow'),
    TzDate32('-144169-01-01,America/Los_Angeles'),
    CAST(Date32('148107-12-31') AS string),
    TzDate32('148107-12-31,UTC'),
    TzDate32('148108-01-01,Europe/Moscow'),
    TzDate32('148107-12-31,America/Los_Angeles'),
    CAST(Date32('1-01-01') AS string),
    TzDate32('1-01-01,UTC'),
    TzDate32('1-01-02,Europe/Moscow'),
    CAST(Date32('-1-12-31') AS string),
    TzDate32('-1-12-31,UTC'),
    TzDate32('1-01-01,Europe/Moscow')
;

SELECT
    CAST(Datetime64('-144169-01-01T00:00:00Z') AS string),
    TzDatetime64('-144169-01-01T00:00:00,UTC'),
    TzDatetime64('-144169-01-01T02:30:17,Europe/Moscow'),
    TzDatetime64('-144170-12-31T16:07:02,America/Los_Angeles'),
    CAST(Datetime64('148107-12-31T23:59:59Z') AS string),
    TzDatetime64('148107-12-31T23:59:59,UTC'),
    TzDatetime64('148108-01-01T02:59:59,Europe/Moscow'),
    TzDatetime64('148107-12-31T15:59:59,America/Los_Angeles'),
    CAST(Datetime64('1-01-01T00:00:00Z') AS string),
    TzDatetime64('1-01-01T00:00:00,UTC'),
    TzDatetime64('1-01-01T02:30:17,Europe/Moscow'),
    CAST(Datetime64('-1-12-31T23:59:59Z') AS string),
    TzDatetime64('-1-12-31T23:59:59,UTC'),
    TzDatetime64('1-01-01T02:30:16,Europe/Moscow')
;

SELECT
    CAST(Timestamp64('-144169-01-01T00:00:00Z') AS string),
    TzTimestamp64('-144169-01-01T00:00:00,UTC'),
    TzTimestamp64('-144169-01-01T02:30:17,Europe/Moscow'),
    TzTimestamp64('-144170-12-31T16:07:02,America/Los_Angeles'),
    CAST(Timestamp64('148107-12-31T23:59:59.999999Z') AS string),
    TzTimestamp64('148107-12-31T23:59:59.999999,UTC'),
    TzTimestamp64('148108-01-01T02:59:59.999999,Europe/Moscow'),
    TzTimestamp64('148107-12-31T15:59:59.999999,America/Los_Angeles'),
    CAST(Timestamp64('1-01-01T00:00:00Z') AS string),
    TzTimestamp64('1-01-01T00:00:00,UTC'),
    TzTimestamp64('1-01-01T02:30:17,Europe/Moscow'),
    CAST(Timestamp64('-1-12-31T23:59:59.999999Z') AS string),
    TzTimestamp64('-1-12-31T23:59:59.999999,UTC'),
    TzTimestamp64('1-01-01T02:30:16.999999,Europe/Moscow')
;
