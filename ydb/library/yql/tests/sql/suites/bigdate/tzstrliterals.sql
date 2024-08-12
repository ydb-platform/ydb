select 
    cast(Date32("-144169-01-01") as string),
    TzDate32("-144169-01-01,UTC"),
    TzDate32("-144169-01-02,Europe/Moscow"),
    TzDate32("-144169-01-01,America/Los_Angeles"),
    cast(Date32("148107-12-31") as string),
    TzDate32("148107-12-31,UTC"),
    TzDate32("148108-01-01,Europe/Moscow"),
    TzDate32("148107-12-31,America/Los_Angeles"),
    cast(Date32("1-01-01") as string),
    TzDate32("1-01-01,UTC"),
    TzDate32("1-01-02,Europe/Moscow"),
    cast(Date32("-1-12-31") as string),
    TzDate32("-1-12-31,UTC"),
    TzDate32("1-01-01,Europe/Moscow");

select 
    cast(Datetime64("-144169-01-01T00:00:00Z") as string),
    TzDatetime64("-144169-01-01T00:00:00,UTC"),
    TzDatetime64("-144169-01-01T02:30:17,Europe/Moscow"),
    TzDatetime64("-144170-12-31T16:07:02,America/Los_Angeles"),
    cast(Datetime64("148107-12-31T23:59:59Z") as string),
    TzDatetime64("148107-12-31T23:59:59,UTC"),
    TzDatetime64("148108-01-01T02:59:59,Europe/Moscow"),
    TzDatetime64("148107-12-31T15:59:59,America/Los_Angeles"),
    cast(Datetime64("1-01-01T00:00:00Z") as string),
    TzDatetime64("1-01-01T00:00:00,UTC"),
    TzDatetime64("1-01-01T02:30:17,Europe/Moscow"),
    cast(Datetime64("-1-12-31T23:59:59Z") as string),
    TzDatetime64("-1-12-31T23:59:59,UTC"),
    TzDatetime64("1-01-01T02:30:16,Europe/Moscow");
    
select 
    cast(Timestamp64("-144169-01-01T00:00:00Z") as string),
    TzTimestamp64("-144169-01-01T00:00:00,UTC"),
    TzTimestamp64("-144169-01-01T02:30:17,Europe/Moscow"),
    TzTimestamp64("-144170-12-31T16:07:02,America/Los_Angeles"),
    cast(Timestamp64("148107-12-31T23:59:59.999999Z") as string),
    TzTimestamp64("148107-12-31T23:59:59.999999,UTC"),
    TzTimestamp64("148108-01-01T02:59:59.999999,Europe/Moscow"),
    TzTimestamp64("148107-12-31T15:59:59.999999,America/Los_Angeles"),
    cast(Timestamp64("1-01-01T00:00:00Z") as string),
    TzTimestamp64("1-01-01T00:00:00,UTC"),
    TzTimestamp64("1-01-01T02:30:17,Europe/Moscow"),
    cast(Timestamp64("-1-12-31T23:59:59.999999Z") as string),
    TzTimestamp64("-1-12-31T23:59:59.999999,UTC"),
    TzTimestamp64("1-01-01T02:30:16.999999,Europe/Moscow");
    
