/* syntax version 1 */
$check = ($arg) -> {
    return <|
        upyear:   CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, 2025)) AS String),
        inyear:   CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, 148108 as Year)) AS String),
        upmonth:  CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, NULL, 2)) AS String),
        inmonth:  CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, 13 as Month)) AS String),
        upday:    CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, NULL, NULL, 17)) AS String),
        inday:    CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, 32 as Day)) AS String),
        update:   CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, 2025, 2, 17)) AS String),
        ipdate:   CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, 2025, 2, 29)) AS String),
        uptime:   CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, NULL, NULL, NULL, 19, 24, 9)) AS String),
        iptime:   CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, NULL, NULL, NULL, 25, 60, 61)) AS String),
        unhour:   CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, 19 as Hour)) AS String),
        inhour:   CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, 24 as Hour)) AS String),
        unminute: CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, 24 as Minute)) AS String),
        inminute: CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, 60 as Minute)) AS String),
        unsecond: CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, 9 as Second)) AS String),
        insecond: CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, 60 as Second)) AS String),
        unmsec:   CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, 123456 as Microsecond)) AS String),
        inmsec:   CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, 1234567 as Microsecond)) AS String),
        untzid:   CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, 1 as TimezoneId)) AS String),
        intzid:   CAST(DateTime::MakeTimestamp64(DateTime::Update($arg, 1000 as TimezoneId)) AS String),
    |>
};

$typeDispatcher = ($row) -> {
    $tm = $row.tm;
    return <|
        explicit: $check(DateTime::Split($tm)),
        implicit: $check($tm),
    |>;
};

$input = SELECT CAST(ftimestamp64 AS Timestamp64) as tm FROM Input;

PROCESS $input USING $typeDispatcher(TableRow());
