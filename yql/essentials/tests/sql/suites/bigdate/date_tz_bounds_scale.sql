/* postgres can not */
select
    CAST(TzDate32("-144169-01-02,Europe/Moscow") as TzDatetime64),
    CAST(TzDate32("-144169-01-01,America/Los_Angeles") as TzDatetime64),
    CAST(TzDate32("148107-12-31,Europe/Moscow") as TzDatetime64),
    CAST(TzDate32("148107-12-31,America/Los_Angeles") as TzDatetime64),

    CAST(TzDate32("-144169-01-02,Europe/Moscow") as TzTimestamp64),
    CAST(TzDate32("-144169-01-01,America/Los_Angeles") as TzTimestamp64),
    CAST(TzDate32("148107-12-31,Europe/Moscow") as TzTimestamp64),
    CAST(TzDate32("148107-12-31,America/Los_Angeles") as TzTimestamp64),

    CAST(TzDatetime64("-144169-01-02T00:00:00,Europe/Moscow") as TzDate32),
    CAST(TzDatetime64("-144169-01-01T00:00:00,America/Los_Angeles") as TzDate32),
    CAST(TzDatetime64("148108-01-01T02:59:59,Europe/Moscow") as TzDate32),
    CAST(TzDatetime64("148107-12-31T15:59:59,America/Los_Angeles") as TzDate32),

    CAST(TzDatetime64("-144169-01-01T02:30:17,Europe/Moscow") as TzTimestamp64),
    CAST(TzDatetime64("-144170-12-31T16:07:02,America/Los_Angeles") as TzTimestamp64),
    CAST(TzDatetime64("148108-01-01T02:59:59,Europe/Moscow") as TzTimestamp64),
    CAST(TzDatetime64("148107-12-31T15:59:59,America/Los_Angeles") as TzTimestamp64),

    CAST(TzTimestamp64("-144169-01-02T00:00:00.000000,Europe/Moscow") as TzDate32),
    CAST(TzTimestamp64("-144169-01-01T00:00:00.000000,America/Los_Angeles") as TzDate32),
    CAST(TzTimestamp64("148108-01-01T02:59:59.999999,Europe/Moscow") as TzDate32),
    CAST(TzTimestamp64("148107-12-31T15:59:59.999999,America/Los_Angeles") as TzDate32),

    CAST(TzTimestamp64("-144169-01-01T02:30:17.000000,Europe/Moscow") as TzDatetime64),
    CAST(TzTimestamp64("-144170-12-31T16:07:02.000000,America/Los_Angeles") as TzDatetime64),
    CAST(TzTimestamp64("148108-01-01T02:59:59.999999,Europe/Moscow") as TzDatetime64),
    CAST(TzTimestamp64("148107-12-31T15:59:59.999999,America/Los_Angeles") as TzDatetime64)
