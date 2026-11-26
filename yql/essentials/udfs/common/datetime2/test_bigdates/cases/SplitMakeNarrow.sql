-- Use casts throughout this boundary fixture to preserve NULL for out-of-range inputs.
$rows = [
    <|fdate: CAST("1969-12-31" AS Date), fdatetime: CAST("1969-12-31T23:59:59Z" AS Datetime), ftimestamp: CAST("1969-12-31T23:59:59.999999Z" AS Timestamp), ftzpdate: CAST("1969-12-31,Europe/Moscow" AS TzDate), ftzpdatetime: CAST("1969-12-31T23:59:59,Europe/Moscow" AS TzDatetime), ftzptimestamp: CAST("1969-12-31T23:59:59.999999,Europe/Moscow" AS TzTimestamp), ftzmdate: CAST("1969-12-31,America/Los_Angeles" AS TzDate), ftzmdatetime: CAST("1969-12-31T23:59:59,America/Los_Angeles" AS TzDatetime), ftzmtimestamp: CAST("1969-12-31T23:59:59.999999,America/Los_Angeles" AS TzTimestamp)|>,
    <|fdate: CAST("1970-01-01" AS Date), fdatetime: CAST("1970-01-01T00:00:00Z" AS Datetime), ftimestamp: CAST("1970-01-01T00:00:00.000000Z" AS Timestamp), ftzpdate: CAST("1970-01-01,Europe/Moscow" AS TzDate), ftzpdatetime: CAST("1970-01-01T00:00:00,Europe/Moscow" AS TzDatetime), ftzptimestamp: CAST("1970-01-01T00:00:00.000000,Europe/Moscow" AS TzTimestamp), ftzmdate: CAST("1970-01-01,America/Los_Angeles" AS TzDate), ftzmdatetime: CAST("1970-01-01T00:00:00,America/Los_Angeles" AS TzDatetime), ftzmtimestamp: CAST("1970-01-01T00:00:00.000000,America/Los_Angeles" AS TzTimestamp)|>,
    <|fdate: CAST("2025-01-01" AS Date), fdatetime: CAST("2025-01-01T00:00:00Z" AS Datetime), ftimestamp: CAST("2025-01-01T00:00:00.000000Z" AS Timestamp), ftzpdate: CAST("2025-01-01,Europe/Moscow" AS TzDate), ftzpdatetime: CAST("2025-01-01T00:00:00,Europe/Moscow" AS TzDatetime), ftzptimestamp: CAST("2025-01-01T00:00:00.000000,Europe/Moscow" AS TzTimestamp), ftzmdate: CAST("2025-01-01,America/Los_Angeles" AS TzDate), ftzmdatetime: CAST("2025-01-01T00:00:00,America/Los_Angeles" AS TzDatetime), ftzmtimestamp: CAST("2025-01-01T00:00:00.000000,America/Los_Angeles" AS TzTimestamp)|>,
    <|fdate: CAST("2105-12-31" AS Date), fdatetime: CAST("2105-12-31T23:59:59Z" AS Datetime), ftimestamp: CAST("2105-12-31T23:59:59.999999Z" AS Timestamp), ftzpdate: CAST("2105-12-31,Europe/Moscow" AS TzDate), ftzpdatetime: CAST("2105-12-31T23:59:59,Europe/Moscow" AS TzDatetime), ftzptimestamp: CAST("2105-12-31T23:59:59.999999,Europe/Moscow" AS TzTimestamp), ftzmdate: CAST("2105-12-31,America/Los_Angeles" AS TzDate), ftzmdatetime: CAST("2105-12-31T23:59:59,America/Los_Angeles" AS TzDatetime), ftzmtimestamp: CAST("2105-12-31T23:59:59.999999,America/Los_Angeles" AS TzTimestamp)|>,
    <|fdate: CAST("2106-01-01" AS Date), fdatetime: CAST("2106-01-01T00:00:00Z" AS Datetime), ftimestamp: CAST("2106-01-01T00:00:00.000000Z" AS Timestamp), ftzpdate: CAST("2106-01-01,Europe/Moscow" AS TzDate), ftzpdatetime: CAST("2106-01-01T00:00:00,Europe/Moscow" AS TzDatetime), ftzptimestamp: CAST("2106-01-01T00:00:00.000000,Europe/Moscow" AS TzTimestamp), ftzmdate: CAST("2106-01-01,America/Los_Angeles" AS TzDate), ftzmdatetime: CAST("2106-01-01T00:00:00,America/Los_Angeles" AS TzDatetime), ftzmtimestamp: CAST("2106-01-01T00:00:00.000000,America/Los_Angeles" AS TzTimestamp)|>,
];

SELECT
    DateTime::MakeDate32(DateTime::Split(fdate)) AS rdate32,
    DateTime::MakeDatetime64(DateTime::Split(fdatetime)) AS rdatetime64,
    DateTime::MakeTimestamp64(DateTime::Split(ftimestamp)) AS rtimestamp64,
    DateTime::MakeTzDate32(DateTime::Split(ftzpdate)) AS rtzpdate32,
    DateTime::MakeTzDatetime64(DateTime::Split(ftzpdatetime)) AS rtzpdatetime64,
    DateTime::MakeTzTimestamp64(DateTime::Split(ftzptimestamp)) AS rtzptimestamp64,
    DateTime::MakeTzDate32(DateTime::Split(ftzmdate)) AS rtzmdate32,
    DateTime::MakeTzDatetime64(DateTime::Split(ftzmdatetime)) AS rtzmdatetime64,
    DateTime::MakeTzTimestamp64(DateTime::Split(ftzmtimestamp)) AS rtzmtimestamp64,
FROM AS_TABLE($rows);
