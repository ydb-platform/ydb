select 
    cast(TzDate32("1900-01-01,Europe/Moscow") as String),
    cast("1900-01-01,Europe/Moscow" as TzDate32),
    cast(TzDatetime64("1900-01-01T01:02:03,Europe/Moscow") as String),
    cast("1900-01-01T01:02:03,Europe/Moscow" as TzDatetime64),
    cast(TzTimestamp64("1900-01-01T01:02:03.456789,Europe/Moscow") as String),
    cast("1900-01-01T01:02:03.456789,Europe/Moscow" as TzTimestamp64),
    
    AddTimezone(Date32("1900-01-01"),"Europe/Moscow"),
    AddTimezone(Datetime64("1900-01-01T01:02:03Z"),"Europe/Moscow"),
    AddTimezone(Timestamp64("1900-01-01T01:02:03.456789Z"),"Europe/Moscow"),
    
    cast(RemoveTimezone(TzDate32("1900-01-02,Europe/Moscow")) as String),
    cast(RemoveTimezone(TzDatetime64("1900-01-01T03:32:20,Europe/Moscow")) as String),
    cast(RemoveTimezone(TzTimestamp64("1900-01-01T03:32:20.456789,Europe/Moscow")) as String);
    
    

