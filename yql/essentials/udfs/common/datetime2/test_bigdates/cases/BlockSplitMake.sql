$rows = [
    <|fdate32: Date32("-1-01-01"), fdatetime64: Datetime64("-1-01-01T23:59:59Z"), ftimestamp64: Timestamp64("-1-01-01T23:59:59.999999Z"), ftzpdate32: TzDate32("-1-01-01,Europe/Moscow"), ftzpdatetime64: TzDatetime64("-1-01-01T23:59:59,Europe/Moscow"), ftzptimestamp64: TzTimestamp64("-1-01-01T23:59:59.999999,Europe/Moscow"), ftzmdate32: TzDate32("-1-01-01,America/Los_Angeles"), ftzmdatetime64: TzDatetime64("-1-01-01T23:59:59,America/Los_Angeles"), ftzmtimestamp64: TzTimestamp64("-1-01-01T23:59:59.999999,America/Los_Angeles")|>,
    <|fdate32: Date32("1-01-01"), fdatetime64: Datetime64("1-01-01T00:00:00Z"), ftimestamp64: Timestamp64("1-01-01T00:00:00.000000Z"), ftzpdate32: TzDate32("1-01-01,Europe/Moscow"), ftzpdatetime64: TzDatetime64("1-01-01T00:00:00,Europe/Moscow"), ftzptimestamp64: TzTimestamp64("1-01-01T00:00:00.000000,Europe/Moscow"), ftzmdate32: TzDate32("1-01-01,America/Los_Angeles"), ftzmdatetime64: TzDatetime64("1-01-01T00:00:00,America/Los_Angeles"), ftzmtimestamp64: TzTimestamp64("1-01-01T00:00:00.000000,America/Los_Angeles")|>,
    <|fdate32: Date32("1969-12-31"), fdatetime64: Datetime64("1969-12-31T23:59:59Z"), ftimestamp64: Timestamp64("1969-12-31T23:59:59.999999Z"), ftzpdate32: TzDate32("1969-12-31,Europe/Moscow"), ftzpdatetime64: TzDatetime64("1969-12-31T23:59:59,Europe/Moscow"), ftzptimestamp64: TzTimestamp64("1969-12-31T23:59:59.999999,Europe/Moscow"), ftzmdate32: TzDate32("1969-12-31,America/Los_Angeles"), ftzmdatetime64: TzDatetime64("1969-12-31T23:59:59,America/Los_Angeles"), ftzmtimestamp64: TzTimestamp64("1969-12-31T23:59:59.999999,America/Los_Angeles")|>,
    <|fdate32: Date32("1970-01-01"), fdatetime64: Datetime64("1970-01-01T00:00:00Z"), ftimestamp64: Timestamp64("1970-01-01T00:00:00.000000Z"), ftzpdate32: TzDate32("1970-01-01,Europe/Moscow"), ftzpdatetime64: TzDatetime64("1970-01-01T00:00:00,Europe/Moscow"), ftzptimestamp64: TzTimestamp64("1970-01-01T00:00:00.000000,Europe/Moscow"), ftzmdate32: TzDate32("1970-01-01,America/Los_Angeles"), ftzmdatetime64: TzDatetime64("1970-01-01T00:00:00,America/Los_Angeles"), ftzmtimestamp64: TzTimestamp64("1970-01-01T00:00:00.000000,America/Los_Angeles")|>,
    <|fdate32: Date32("2025-01-01"), fdatetime64: Datetime64("2025-01-01T00:00:00Z"), ftimestamp64: Timestamp64("2025-01-01T00:00:00.000000Z"), ftzpdate32: TzDate32("2025-01-01,Europe/Moscow"), ftzpdatetime64: TzDatetime64("2025-01-01T00:00:00,Europe/Moscow"), ftzptimestamp64: TzTimestamp64("2025-01-01T00:00:00.000000,Europe/Moscow"), ftzmdate32: TzDate32("2025-01-01,America/Los_Angeles"), ftzmdatetime64: TzDatetime64("2025-01-01T00:00:00,America/Los_Angeles"), ftzmtimestamp64: TzTimestamp64("2025-01-01T00:00:00.000000,America/Los_Angeles")|>,
    <|fdate32: Date32("2106-01-01"), fdatetime64: Datetime64("2106-01-01T00:00:00Z"), ftimestamp64: Timestamp64("2106-01-01T00:00:00.000000Z"), ftzpdate32: TzDate32("2106-01-01,Europe/Moscow"), ftzpdatetime64: TzDatetime64("2106-01-01T00:00:00,Europe/Moscow"), ftzptimestamp64: TzTimestamp64("2106-01-01T00:00:00.000000,Europe/Moscow"), ftzmdate32: TzDate32("2106-01-01,America/Los_Angeles"), ftzmdatetime64: TzDatetime64("2106-01-01T00:00:00,America/Los_Angeles"), ftzmtimestamp64: TzTimestamp64("2106-01-01T00:00:00.000000,America/Los_Angeles")|>,
];

pragma UseBlocks;

SELECT
    DateTime::MakeDate32(fdate32) AS rdate32,
    DateTime::MakeDatetime64(fdatetime64) AS rdatetime64,
    DateTime::MakeTimestamp64(ftimestamp64) AS rtimestamp64,
    DateTime::MakeTzDate32(ftzpdate32) AS rtzpdate32,
    DateTime::MakeTzDatetime64(ftzpdatetime64) AS rtzpdatetime64,
    DateTime::MakeTzTimestamp64(ftzptimestamp64) AS rtzptimestamp64,
    DateTime::MakeTzDate32(ftzmdate32) AS rtzmdate32,
    DateTime::MakeTzDatetime64(ftzmdatetime64) AS rtzmdatetime64,
    DateTime::MakeTzTimestamp64(ftzmtimestamp64) AS rtzmtimestamp64,
FROM AS_TABLE($rows);
