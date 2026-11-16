$rows = [
    <|ftztimestamp: Just(TzTimestamp("1970-01-01T05:00:00.000000,Europe/Moscow"))|>,
    <|ftztimestamp: Just(TzTimestamp("2018-12-15T01:02:03.456789,Europe/Moscow"))|>,
    <|ftztimestamp: Just(TzTimestamp("2105-12-31T16:23:45.000000,GMT"))|>,
    <|ftztimestamp: Just(TzTimestamp("2106-01-01T01:00:00.000000,Europe/Moscow"))|>,
    <|ftztimestamp: Just(TzTimestamp("2019-07-24T12:00:00,Europe/Moscow"))|>,
];

$format = DateTime::Format("%Y-%m-%d %H:%M:%S %Z");

select
    $format(DateTime::EndOfMonth(TzDateTime('2023-07-07T01:02:03,Europe/Moscow'))),
    $format(DateTime::EndOfMonth(Date('2023-08-08'))),
    $format(DateTime::EndOfMonth(Date('2023-09-09'))),
    $format(DateTime::EndOfMonth(Date('2023-02-02'))),
    $format(DateTime::EndOfMonth(Date('2024-02-02')))
into result `Normal cases`;

select $format(CAST('1970-01-01T00:00:00.000000Z' AS Timestamp))
    , $format(DateTime::EndOfMonth(CAST('1970-01-01T00:00:00.000000Z' AS Timestamp)))
    , $format(DateTime::EndOfMonth(cast('1970-01-01T00:00:00.000000,Atlantic/Madeira' as Timestamp)))
into result `Minimal timestamp value`;

select $format(CAST('2105-12-31T23:59:59.999999Z' AS Timestamp))
    , $format(DateTime::EndOfMonth(CAST('2105-12-31T23:59:59.999999Z' AS Timestamp)))
    , $format(DateTime::EndOfMonth(CAST('2105-12-12T00:00:00Z' AS Timestamp)))
    , $format(DateTime::EndOfMonth(cast('2105-12-31T23:59:59.999999,Atlantic/Azores' as Timestamp)))
into result `Maximum timestamp value`;

select $format(CAST('1969-12-31T23:59:59.999999,Atlantic/Azores' AS TzTimestamp))
    , $format(DateTime::EndOfMonth(CAST('1969-12-31T23:59:59.999999,Atlantic/Azores' AS TzTimestamp)))
into result `Timestamp below minimum`;

select $format(cast('2106-01-01T00:00:00.000000,Atlantic/Madeira' as TzTimestamp))
    , $format(DateTime::EndOfMonth(cast('2106-01-01T00:00:00.000000,Atlantic/Madeira' as TzTimestamp)))
into result `Timestamp above maximum`;

$check = ($arg) -> {
    return <|
        soyear:    DateTime::EndOfYear($arg),
        soquarter: DateTime::EndOfQuarter($arg),
        somonth:   DateTime::EndOfMonth($arg),
        soweek:    DateTime::EndOfWeek($arg),
        soday:     DateTime::EndOfDay($arg),
        sopt13h:   DateTime::EndOf($arg, Interval("PT13H")),
        sopt4h:    DateTime::EndOf($arg, Interval("PT4H")),
        sopt15m:   DateTime::EndOf($arg, Interval("PT15M")),
        sopt20s:   DateTime::EndOf($arg, Interval("PT20S")),
        sopt7s:    DateTime::EndOf($arg, Interval("PT7S")),
    |>
};

$typeDispatcher = ($row) -> {
    $tm = $row.ftztimestamp;
    return <|
        explicit: $check(DateTime::Split($tm)),
        implicit: $check($tm),
    |>;
};

PROCESS AS_TABLE($rows) USING $typeDispatcher(TableRow()) into result `Other cases`;
