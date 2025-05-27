/* postgres can not */
select 
    cast(TZDATE("1970-01-02,Europe/Moscow") as TzDatetime),
    cast(TZDATE("1970-01-01,America/Los_Angeles") as TzDatetime),
    cast(TZDATE("2105-12-31,Europe/Moscow") as TzDatetime),
    cast(TZDATE("2105-12-31,America/Los_Angeles") as TzDatetime),

    cast(TZDATE("1970-01-02,Europe/Moscow") as TzTimestamp),
    cast(TZDATE("1970-01-01,America/Los_Angeles") as TzTimestamp),
    cast(TZDATE("2105-12-31,Europe/Moscow") as TzTimestamp),
    cast(TZDATE("2105-12-31,America/Los_Angeles") as TzTimestamp),

    cast(TZDATETIME("1970-01-02T00:00:00,Europe/Moscow") as TzDate),
    cast(TZDATETIME("1970-01-01T00:00:00,America/Los_Angeles") as TzDate),
    cast(TZDATETIME("2106-01-01T02:59:59,Europe/Moscow") as TzDate),
    cast(TZDATETIME("2105-12-31T15:59:59,America/Los_Angeles") as TzDate),

    cast(TZDATETIME("1970-01-02T00:00:00,Europe/Moscow") as TzTimestamp),
    cast(TZDATETIME("1970-01-01T00:00:00,America/Los_Angeles") as TzTimestamp),
    cast(TZDATETIME("2106-01-01T02:59:59,Europe/Moscow") as TzTimestamp),
    cast(TZDATETIME("2105-12-31T15:59:59,America/Los_Angeles") as TzTimestamp),

    cast(TZTIMESTAMP("1970-01-02T00:00:00.000000,Europe/Moscow") as TzDate),
    cast(TZTIMESTAMP("1970-01-01T00:00:00.000000,America/Los_Angeles") as TzDate),
    cast(TZTIMESTAMP("2106-01-01T02:59:59.999999,Europe/Moscow") as TzDate),
    cast(TZTIMESTAMP("2105-12-31T15:59:59.999999,America/Los_Angeles") as TzDate),

    cast(TZTIMESTAMP("1970-01-02T00:00:00.000000,Europe/Moscow") as TzDatetime),
    cast(TZTIMESTAMP("1970-01-01T00:00:00.000000,America/Los_Angeles") as TzDatetime),
    cast(TZTIMESTAMP("2106-01-01T02:59:59.999999,Europe/Moscow") as TzDatetime),
    cast(TZTIMESTAMP("2105-12-31T15:59:59.999999,America/Los_Angeles") as TzDatetime)
