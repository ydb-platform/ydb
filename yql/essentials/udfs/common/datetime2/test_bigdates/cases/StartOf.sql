$rows = [
    <|ftztimestamp64: Just(TzTimestamp64("-144169-01-01T01:02:03.456789,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("-1-01-01T23:58:57.654321,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("1-01-01T09:09:09.000009,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("1969-12-31T23:58:57.654321,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("1970-01-01T09:09:09.000009,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("2025-02-04T09:09:09.000009,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("2106-01-01T09:09:09.000009,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("148107-12-31T23:58:57.654321,GMT"))|>,
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
    $tm = $row.ftztimestamp64;
    return <|
        explicit: $check(DateTime::Split($tm)),
        implicit: $check($tm),
    |>;
};

PROCESS AS_TABLE($rows) USING $typeDispatcher(TableRow());
