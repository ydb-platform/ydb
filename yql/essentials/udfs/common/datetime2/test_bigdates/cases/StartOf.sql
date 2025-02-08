/* syntax version 1 */
$check = ($arg) -> {
    return <|
        soyear:    DateTime::MakeTimestamp64(DateTime::StartOfYear($arg)),
        soquarter: DateTime::MakeTimestamp64(DateTime::StartOfQuarter($arg)),
        somonth:   DateTime::MakeTimestamp64(DateTime::StartOfMonth($arg)),
        soweek:    DateTime::MakeTimestamp64(DateTime::StartOfWeek($arg)),
        soday:     DateTime::MakeTimestamp64(DateTime::StartOfDay($arg)),
    |>
};

$typeDispatcher = ($row) -> {
    $tm = $row.tm;
    return <|
        explicit: $check(DateTime::Split($tm)),
        implicit: $check($tm),
    |>;
};

$input = SELECT CAST(ftimestamp64 as Timestamp64) as tm FROM Input;

PROCESS $input USING $typeDispatcher(TableRow());
