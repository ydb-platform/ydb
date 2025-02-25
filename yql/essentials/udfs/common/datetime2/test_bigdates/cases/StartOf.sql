/* syntax version 1 */
$check = ($arg) -> {
    return <|
        soyear:    DateTime::MakeTimestamp64(DateTime::StartOfYear($arg)),
        soquarter: DateTime::MakeTimestamp64(DateTime::StartOfQuarter($arg)),
        somonth:   DateTime::MakeTimestamp64(DateTime::StartOfMonth($arg)),
        soweek:    DateTime::MakeTimestamp64(DateTime::StartOfWeek($arg)),
        soday:     DateTime::MakeTimestamp64(DateTime::StartOfDay($arg)),
        sopt13h:   DateTime::MakeTimestamp64(DateTime::StartOf($arg, Interval("PT13H"))),
        sopt4h:    DateTime::MakeTimestamp64(DateTime::StartOf($arg, Interval("PT4H"))),
        sopt15m:   DateTime::MakeTimestamp64(DateTime::StartOf($arg, Interval("PT15M"))),
        sopt20s:   DateTime::MakeTimestamp64(DateTime::StartOf($arg, Interval("PT20S"))),
        sopt7s:    DateTime::MakeTimestamp64(DateTime::StartOf($arg, Interval("PT7S"))),
        timeofday: DateTime::TimeOfDay($arg),
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
