$rows = [
    <|ftztimestamp: Just(TzTimestamp("1970-01-01T05:00:00.000000,Europe/Moscow"))|>,
    <|ftztimestamp: Just(TzTimestamp("2018-12-15T01:02:03.456789,Europe/Moscow"))|>,
    <|ftztimestamp: Just(TzTimestamp("2105-12-31T16:23:45.000000,GMT"))|>,
    <|ftztimestamp: Just(TzTimestamp("2106-01-01T01:00:00.000000,Europe/Moscow"))|>,
    <|ftztimestamp: Just(TzTimestamp("2019-07-24T12:00:00,Europe/Moscow"))|>,
];

$check = ($arg) -> {
    return <|
        soyear:    DateTime::StartOfYear($arg),
        soquarter: DateTime::StartOfQuarter($arg),
        somonth:   DateTime::StartOfMonth($arg),
        soweek:    DateTime::StartOfWeek($arg),
        soday:     DateTime::StartOfDay($arg),
        sopt13h:   DateTime::StartOf($arg, Interval("PT13H")),
        sopt4h:    DateTime::StartOf($arg, Interval("PT4H")),
        sopt15m:   DateTime::StartOf($arg, Interval("PT15M")),
        sopt20s:   DateTime::StartOf($arg, Interval("PT20S")),
        sopt7s:    DateTime::StartOf($arg, Interval("PT7S")),
        timeofday: DateTime::TimeOfDay($arg),
    |>
};

$typeDispatcher = ($row) -> {
    $tm = $row.ftztimestamp;
    return <|
        explicit: $check(DateTime::Split($tm)),
        implicit: $check($tm),
    |>;
};

PROCESS AS_TABLE($rows) USING $typeDispatcher(TableRow());
