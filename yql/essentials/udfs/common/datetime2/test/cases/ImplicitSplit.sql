$rows = [
    <|fdate: Just(Date("2018-12-15")), fdatetime: Just(Datetime("2018-12-15T01:02:03Z")), ftimestamp: Just(Timestamp("2018-12-15T01:02:03.456789Z")), ftzdate: Just(TzDate("2018-12-15,Europe/Moscow")), ftzdatetime: Just(TzDatetime("2018-12-15T01:02:03,Europe/Moscow")), ftztimestamp: Just(TzTimestamp("2018-12-15T01:02:03.456789,Europe/Moscow"))|>,
];

$format = DateTime::Format("%Y%m%d %H%M%S %Z");

select
    $format(fdate),
    $format(fdatetime),
    $format(ftimestamp),
    $format(ftzdate),
    $format(ftzdatetime),
    $format(ftztimestamp)
FROM AS_TABLE($rows);
