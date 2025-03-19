/* syntax version 1 */
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
    $tm = $row.tm;
    return <|
        explicit: $check(DateTime::Split($tm)),
        implicit: $check($tm),
    |>;
};

$input = SELECT CAST(ftztimestamp64 as TzTimestamp64) as tm FROM Input;

PROCESS $input USING $typeDispatcher(TableRow());
