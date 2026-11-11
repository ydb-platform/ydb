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
        eoyear:    DateTime::EndOfYear($arg),
        eoquarter: DateTime::EndOfQuarter($arg),
        eomonth:   DateTime::EndOfMonth($arg),
        eoweek:    DateTime::EndOfWeek($arg),
        eoday:     DateTime::EndOfDay($arg),
        eopt13h:   DateTime::EndOf($arg, Interval("PT13H")),
        eopt4h:    DateTime::EndOf($arg, Interval("PT4H")),
        eopt15m:   DateTime::EndOf($arg, Interval("PT15M")),
        eopt20s:   DateTime::EndOf($arg, Interval("PT20S")),
        eopt7s:    DateTime::EndOf($arg, Interval("PT7S")),
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
