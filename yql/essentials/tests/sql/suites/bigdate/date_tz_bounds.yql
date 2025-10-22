/* postgres can not */
SELECT
    TzDate32("-144169-01-02,Europe/Moscow"),
    TzDate32("-144169-01-01,America/Los_Angeles"),
    TzDate32("148107-12-31,Europe/Moscow"),
    TzDate32("148107-12-31,America/Los_Angeles"),

    -- This boundary is adjusted to minimal UTC seconds, according
    -- to Europe/Moscow offset discrepancy for negative dates.
    TzDatetime64("-144169-01-01T02:30:17,Europe/Moscow"),
    -- This boundary is adjusted to minimal UTC seconds, according
    -- to America/Los_Angeles offset discrepancy for negative dates.
    TzDatetime64("-144170-12-31T16:07:02,America/Los_Angeles"),
    TzDatetime64("148108-01-01T02:59:59,Europe/Moscow"),
    TzDatetime64("148107-12-31T15:59:59,America/Los_Angeles"),

    -- This boundary is adjusted to minimal UTC seconds, according
    -- to Europe/Moscow offset discrepancy for negative dates.
    TzTimestamp64("-144169-01-01T02:30:17.000000,Europe/Moscow"),
    -- This boundary is adjusted to minimal UTC seconds, according
    -- to America/Los_Angeles offset discrepancy for negative dates.
    TzTimestamp64("-144170-12-31T16:07:02.000000,America/Los_Angeles"),
    TzTimestamp64("148108-01-01T02:59:59.999999,Europe/Moscow"),
    TzTimestamp64("148107-12-31T15:59:59.999999,America/Los_Angeles"),
