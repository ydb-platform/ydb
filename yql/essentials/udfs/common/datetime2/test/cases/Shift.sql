/* syntax version 1 */
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
    $tm = $row.tm;
    return <|
        explicit: $check(DateTime::Split($tm)),
        implicit: $check($tm),
    |>;
};

$input = SELECT CAST(ftztimestamp as TzTimestamp) as tm FROM Input;

PROCESS $input USING $typeDispatcher(TableRow());
