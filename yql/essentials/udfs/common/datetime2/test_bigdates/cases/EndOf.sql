/* syntax version 1 */
$check = ($arg) -> {
    return <|
        eoyear:    DateTime::MakeTimestamp64(DateTime::EndOfYear($arg)),
        eoquarter: DateTime::MakeTimestamp64(DateTime::EndOfQuarter($arg)),
        eomonth:   DateTime::MakeTimestamp64(DateTime::EndOfMonth($arg)),
        eoweek:    DateTime::MakeTimestamp64(DateTime::EndOfWeek($arg)),
        eoday:     DateTime::MakeTimestamp64(DateTime::EndOfDay($arg)),
        sopt13h:   DateTime::MakeTimestamp64(DateTime::EndOf($arg, Interval("PT13H"))),
        sopt4h:    DateTime::MakeTimestamp64(DateTime::EndOf($arg, Interval("PT4H"))),
        sopt15m:   DateTime::MakeTimestamp64(DateTime::EndOf($arg, Interval("PT15M"))),
        sopt20s:   DateTime::MakeTimestamp64(DateTime::EndOf($arg, Interval("PT20S"))),
        sopt7s:    DateTime::MakeTimestamp64(DateTime::EndOf($arg, Interval("PT7S"))),
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
