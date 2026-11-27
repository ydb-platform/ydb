$rows = [
    <|ftztimestamp64: Just(TzTimestamp64("-144169-01-01T00:00:00.000000,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("-1-01-01T23:59:59.999999,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("1-01-01T00:00:00.000000,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("1969-12-31T23:59:59.999999,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("1970-01-01T00:00:00.000000,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("2025-01-01T00:00:00.000000,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("2106-01-01T00:00:00.000000,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("148107-12-31T23:59:59.999999,GMT"))|>,
];

$check = ($arg) -> {
    return <|
        sh10y:   DateTime::ShiftYears($arg, 10),
        sh16q:   DateTime::ShiftQuarters($arg, 16),
        shm16q:  DateTime::ShiftQuarters($arg, -16),
        sh0m:    DateTime::ShiftMonths($arg, 0),
        sh1m:    DateTime::ShiftMonths($arg, 1),
        sh3m:    DateTime::ShiftMonths($arg, 3),
        sh11m:   DateTime::ShiftMonths($arg, 11),
        sh12m:   DateTime::ShiftMonths($arg, 12),
        sh123m:  DateTime::ShiftMonths($arg, 123),
        shm1m:   DateTime::ShiftMonths($arg, -1),
        shm3m:   DateTime::ShiftMonths($arg, -3),
        shm11m:  DateTime::ShiftMonths($arg, -11),
        shm12m:  DateTime::ShiftMonths($arg, -12),
        shm123m: DateTime::ShiftMonths($arg, -123),
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
