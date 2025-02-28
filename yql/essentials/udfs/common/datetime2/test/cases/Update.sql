/* syntax version 1 */
$check = ($arg) -> {
    return <|
        upyear:   CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, 2005)) AS String),
        inyear:   CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, 2200 as Year)) AS String),
        upmonth:  CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, NULL, 7)) AS String),
        inmonth:  CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, 13 as Month)) AS String),
        upday:    CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, NULL, NULL, 20)) AS String),
        inday:    CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, 32 as Day)) AS String),
        ipdate:   CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, 2018, 2, 30)) AS String),
        uptime:   CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, NULL, NULL, NULL, 11, 10, 9)) AS String),
        unhour:   CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, 11 as Hour)) AS String),
        inhour:   CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, 24 as Hour)) AS String),
        unminute: CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, 10 as Minute)) AS String),
        inminute: CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, 60 as Minute)) AS String),
        unsecond: CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, 9 as Second)) AS String),
        insecond: CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, 60 as Second)) AS String),
        unmsec:   CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, 123456 as Microsecond)) AS String),
        inmsec:   CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, 2000000 as Microsecond)) AS String),
        untzid:   CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, 100 as TimezoneId)) AS String),
        intzid:   CAST(DateTime::MakeTzTimestamp(DateTime::Update($arg, 1000 as TimezoneId)) AS String),
    |>
};

$typeDispatcher = ($row) -> {
    $tm = $row.tm;
    return <|
        explicit: $check(DateTime::Split($tm)),
        implicit: $check($tm),
    |>;
};

$input = SELECT CAST(ftztimestamp AS TzTimestamp) as tm FROM Input;

PROCESS $input USING $typeDispatcher(TableRow());
