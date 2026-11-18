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
        upyear:   DateTime::Update($arg, 2025),
        inyear:   DateTime::Update($arg, 148108 as Year),
        upmonth:  DateTime::Update($arg, NULL, 2),
        inmonth:  DateTime::Update($arg, 13 as Month),
        upday:    DateTime::Update($arg, NULL, NULL, 17),
        inday:    DateTime::Update($arg, 32 as Day),
        update:   DateTime::Update($arg, 2025, 2, 17),
        ipdate:   DateTime::Update($arg, 2025, 2, 29),
        uptime:   DateTime::Update($arg, NULL, NULL, NULL, 19, 24, 9),
        iptime:   DateTime::Update($arg, NULL, NULL, NULL, 25, 60, 61),
        unhour:   DateTime::Update($arg, 19 as Hour),
        inhour:   DateTime::Update($arg, 24 as Hour),
        unminute: DateTime::Update($arg, 24 as Minute),
        inminute: DateTime::Update($arg, 60 as Minute),
        unsecond: DateTime::Update($arg, 9 as Second),
        insecond: DateTime::Update($arg, 60 as Second),
        unmsec:   DateTime::Update($arg, 123456 as Microsecond),
        inmsec:   DateTime::Update($arg, 1234567 as Microsecond),
        untzid:   DateTime::Update($arg, 1 as TimezoneId),
        intzid:   DateTime::Update($arg, 1000 as TimezoneId),
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
