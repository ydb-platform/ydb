/* syntax version 1 */
$check = ($arg) -> {
    return <|
        eoyear:    DateTime::EndOfYear($arg),
        eoquarter: DateTime::EndOfQuarter($arg),
        eomonth:   DateTime::EndOfMonth($arg),
        eoweek:    DateTime::EndOfWeek($arg),
        eoday:     DateTime::EndOfDay($arg),
        eopt13h:   DateTime::EndOf($arg, Interval("PT13H")),
        eopt4h:    DateTime::EndOf($arg, Interval("PT4H")),
        eopt15m:   DateTime::EndOf($arg, Interval("PT15M")),
        eopt20s:   DateTime::EndOf($arg, Interval("PT20S")),
        eopt7s:    DateTime::EndOf($arg, Interval("PT7S")),
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
