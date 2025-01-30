/* syntax version 1 */
$check = ($arg) -> {
    return <|
        ryear:              DateTime::GetYear($arg),
        rdayofyear:         DateTime::GetDayOfYear($arg),
        rmonth:             DateTime::GetMonth($arg),
        rweekofyear:        DateTime::GetWeekOfYear($arg),
        rweekofyeariso8601: DateTime::GetWeekOfYearIso8601($arg),
        rdayofmonth:        DateTime::GetDayOfMonth($arg),
        rdayofweek:         DateTime::GetDayOfWeek($arg),
        rtz:                DateTime::GetTimezoneId($arg),
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
