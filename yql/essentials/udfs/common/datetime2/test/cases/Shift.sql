$rows = [
    <|ftztimestamp: Just(TzTimestamp("2001-11-17T21:20:19.345678,GMT"))|>,
    <|ftztimestamp: Just(TzTimestamp("1970-01-01T11:14:00.000000,GMT"))|>,
    <|ftztimestamp: Just(TzTimestamp("2105-12-01T01:08:00.000000,Europe/Moscow"))|>,
    <|ftztimestamp: Just(TzTimestamp("2049-06-13T00:00:00.000000,GMT"))|>,
    <|ftztimestamp: Just(TzTimestamp("2000-01-31T16:15:00.000000,Europe/Uzhgorod"))|>,
    <|ftztimestamp: Just(TzTimestamp("2024-02-29T01:02:03.456789,Europe/Moscow"))|>,
    <|ftztimestamp: Just(TzTimestamp("1970-02-01T02:00:00.444123,Europe/Moscow"))|>,
];

$check = ($arg) -> {
    return <|
        sh10y:   CAST(DateTime::MakeTzTimestamp(DateTime::ShiftYears($arg, 10)) as String),
        sh16q:   CAST(DateTime::MakeTzTimestamp(DateTime::ShiftQuarters($arg, 16)) as String),
        shm16q:  CAST(DateTime::MakeTzTimestamp(DateTime::ShiftQuarters($arg, -16)) as String),
        sh0m:    CAST(DateTime::MakeTzTimestamp(DateTime::ShiftMonths($arg, 0)) as String),
        sh1m:    CAST(DateTime::MakeTzTimestamp(DateTime::ShiftMonths($arg, 1)) as String),
        sh3m:    CAST(DateTime::MakeTzTimestamp(DateTime::ShiftMonths($arg, 3)) as String),
        sh11m:   CAST(DateTime::MakeTzTimestamp(DateTime::ShiftMonths($arg, 11)) as String),
        sh12m:   CAST(DateTime::MakeTzTimestamp(DateTime::ShiftMonths($arg, 12)) as String),
        sh123m:  CAST(DateTime::MakeTzTimestamp(DateTime::ShiftMonths($arg, 123)) as String),
        shm1m:   CAST(DateTime::MakeTzTimestamp(DateTime::ShiftMonths($arg, -1)) as String),
        shm3m:   CAST(DateTime::MakeTzTimestamp(DateTime::ShiftMonths($arg, -3)) as String),
        shm11m:  CAST(DateTime::MakeTzTimestamp(DateTime::ShiftMonths($arg, -11)) as String),
        shm12m:  CAST(DateTime::MakeTzTimestamp(DateTime::ShiftMonths($arg, -12)) as String),
        shm123m: CAST(DateTime::MakeTzTimestamp(DateTime::ShiftMonths($arg, -123)) as String),
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
