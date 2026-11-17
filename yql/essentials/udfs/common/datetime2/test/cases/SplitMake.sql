-- Use casts throughout this boundary fixture to preserve NULL for out-of-range inputs.
$rows = [
    <|fdate: CAST("1945-05-09" AS Date), fdatetime: CAST("1945-05-09T00:00:00Z" AS Datetime), ftimestamp: CAST("1945-05-09T00:00:00.000000Z" AS Timestamp), ftzdate: CAST("1945-05-09,Europe/Moscow" AS TzDate), ftzdatetime: CAST("1945-05-09T00:00:00,Europe/Moscow" AS TzDatetime), ftztimestamp: CAST("1945-05-09T00:00:00.000000,Europe/Moscow" AS TzTimestamp)|>,
    <|fdate: CAST("1970-01-01" AS Date), fdatetime: CAST("1970-01-01T00:30:00Z" AS Datetime), ftimestamp: CAST("1970-01-01T00:30:00.000000Z" AS Timestamp), ftzdate: CAST("1970-01-01,Europe/Moscow" AS TzDate), ftzdatetime: CAST("1970-01-01T01:00:00,Europe/Moscow" AS TzDatetime), ftztimestamp: CAST("1970-01-01T05:00:00.000000,Europe/Moscow" AS TzTimestamp)|>,
    <|fdate: CAST("2018-12-15" AS Date), fdatetime: CAST("2018-12-15T01:02:03Z" AS Datetime), ftimestamp: CAST("2018-12-15T01:02:03.456789Z" AS Timestamp), ftzdate: CAST("2018-12-15,Europe/Moscow" AS TzDate), ftzdatetime: CAST("2018-12-15T01:02:03,Europe/Moscow" AS TzDatetime), ftztimestamp: CAST("2018-12-15T01:02:03.456789,Europe/Moscow" AS TzTimestamp)|>,
    <|fdate: CAST("2105-12-31" AS Date), fdatetime: CAST("2105-12-31T23:59:59Z" AS Datetime), ftimestamp: CAST("2105-12-31T23:59:59.999999Z" AS Timestamp), ftzdate: CAST("2105-12-31,Canada/Central" AS TzDate), ftzdatetime: CAST("2105-12-31T16:00:00,Canada/Central" AS TzDatetime), ftztimestamp: CAST("2105-12-31T23:00:00.000000,Canada/Central" AS TzTimestamp)|>,
    <|fdate: CAST("2106-01-01" AS Date), fdatetime: CAST("2106-01-01T00:00:00Z" AS Datetime), ftimestamp: CAST("2106-01-01T00:00:00.000000Z" AS Timestamp), ftzdate: CAST("2106-01-01,Europe/Moscow" AS TzDate), ftzdatetime: CAST("2106-01-01T01:00:00,Europe/Moscow" AS TzDatetime), ftztimestamp: CAST("2106-01-01T05:00:00.000000,Europe/Moscow" AS TzTimestamp)|>,
    <|fdate: CAST("2117-11-07" AS Date), fdatetime: CAST("2117-11-07T00:00:00Z" AS Datetime), ftimestamp: CAST("2117-11-07T00:00:00.000000Z" AS Timestamp), ftzdate: CAST("2117-11-07,Europe/Moscow" AS TzDate), ftzdatetime: CAST("2117-11-07T00:00:00,Europe/Moscow" AS TzDatetime), ftztimestamp: CAST("2117-11-07T00:00:00.000000,Europe/Moscow" AS TzTimestamp)|>,
];

select
    DateTime::MakeDate(DateTime::Split(fdate)) as rdate,
    DateTime::MakeDatetime(DateTime::Split(fdatetime)) as rdatetime,
    DateTime::MakeTimestamp(DateTime::Split(ftimestamp)) as rtimestamp,
    DateTime::MakeTzDate(DateTime::Split(ftzdate)) as rtzdate,
    DateTime::MakeTzDatetime(DateTime::Split(ftzdatetime)) as rtzdatetime,
    DateTime::MakeTzTimestamp(DateTime::Split(ftztimestamp)) as rtztimestamp
FROM AS_TABLE($rows);
