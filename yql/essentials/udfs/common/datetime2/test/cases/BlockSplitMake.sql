$rows = [
    <|fdate: Date("2018-12-15"), fdatetime: Datetime("2018-12-15T01:02:03Z"), ftimestamp: Timestamp("2018-12-15T01:02:03.456789Z"), ftzdate: TzDate("2018-12-15,Europe/Moscow"), ftzdatetime: TzDatetime("2018-12-15T01:02:03,Europe/Moscow"), ftztimestamp: TzTimestamp("2018-12-15T01:02:03.456789,Europe/Moscow")|>,
    <|fdate: Date("2005-12-31"), fdatetime: Datetime("2005-12-31T23:59:59Z"), ftimestamp: Timestamp("2005-12-31T23:59:59.999999Z"), ftzdate: TzDate("2005-12-31,Canada/Central"), ftzdatetime: TzDatetime("2005-12-31T16:00:00,Canada/Central"), ftztimestamp: TzTimestamp("2005-12-31T23:00:00.000000,Canada/Central")|>,
];

pragma UseBlocks;

select
    DateTime::MakeDate(fdate) as rdate,
    DateTime::MakeDatetime(fdatetime) as rdatetime,
    DateTime::MakeTimestamp(ftimestamp) as rtimestamp,
    DateTime::MakeTzDate(ftzdate) as rtzdate,
    DateTime::MakeTzDatetime(ftzdatetime) as rtzdatetime,
    DateTime::MakeTzTimestamp(ftztimestamp) as rtztimestamp
FROM AS_TABLE($rows);
