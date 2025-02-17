/* postgres can not */
select 
    TZDATE("1970-01-02,Europe/Moscow"),
    TZDATE("1970-01-01,America/Los_Angeles"),
    TZDATE("2105-12-31,Europe/Moscow"),
    TZDATE("2105-12-31,America/Los_Angeles"),

    TZDATETIME("1970-01-01T03:00:00,Europe/Moscow"),
    TZDATETIME("1969-12-31T16:00:00,America/Los_Angeles"),
    TZDATETIME("2106-01-01T02:59:59,Europe/Moscow"),
    TZDATETIME("2105-12-31T15:59:59,America/Los_Angeles"),

    TZTIMESTAMP("1970-01-01T03:00:00.000000,Europe/Moscow"),
    TZTIMESTAMP("1969-12-31T16:00:00.000000,America/Los_Angeles"),
    TZTIMESTAMP("2106-01-01T02:59:59.999999,Europe/Moscow"),
    TZTIMESTAMP("2105-12-31T15:59:59.999999,America/Los_Angeles")

