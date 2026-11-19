-- Use casts throughout this boundary fixture to preserve NULL for out-of-range inputs.
$rows = [
    <|fdate: CAST("1969-12-31" AS Date), fdatetime: CAST("1969-12-31T23:00:00Z" AS Datetime), ftimestamp: CAST("1969-12-31T23:00:00.000000Z" AS Timestamp), ftzdate: CAST("1969-12-31,Canada/Central" AS TzDate), ftzdatetime: CAST("1969-12-31T16:00:00,Canada/Central" AS TzDatetime), ftztimestamp: CAST("1969-12-31T23:00:00.000000,Canada/Central" AS TzTimestamp)|>,
];

select
    DateTime::MakeDate(DateTime::Split(fdate)) as rdate,
    DateTime::MakeDatetime(DateTime::Split(fdatetime)) as rdatetime,
    DateTime::MakeTimestamp(DateTime::Split(ftimestamp)) as rtimestamp,
    DateTime::MakeTzDate(DateTime::Split(ftzdate)) as rtzdate,
    DateTime::MakeTzDatetime(DateTime::Split(ftzdatetime)) as rtzdatetime,
    DateTime::MakeTzTimestamp(DateTime::Split(ftztimestamp)) as rtztimestamp
FROM AS_TABLE($rows);
