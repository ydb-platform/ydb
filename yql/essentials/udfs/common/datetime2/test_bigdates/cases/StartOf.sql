/* syntax version 1 */
$check = ($arg) -> {
    return <|
        soyear:    DateTime::StartOfYear($arg),
        soquarter: DateTime::StartOfQuarter($arg),
        somonth:   DateTime::StartOfMonth($arg),
        soweek:    DateTime::StartOfWeek($arg),
        soday:     DateTime::StartOfDay($arg),
        sopt13h:   DateTime::StartOf($arg, Interval("PT13H")),
        sopt4h:    DateTime::StartOf($arg, Interval("PT4H")),
        sopt15m:   DateTime::StartOf($arg, Interval("PT15M")),
        sopt20s:   DateTime::StartOf($arg, Interval("PT20S")),
        sopt7s:    DateTime::StartOf($arg, Interval("PT7S")),
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

$input = SELECT CAST(ftztimestamp64 as TzTimestamp64) as tm FROM Input;

PROCESS $input USING $typeDispatcher(TableRow());
