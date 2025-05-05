/* syntax version 1 */
$check = ($arg) -> {
    return <|
        ryear:              DateTime::GetYear($arg),
        rdayofyear:         DateTime::GetDayOfYear($arg),
        rmonth:             DateTime::GetMonth($arg),
        rmonthname:         DateTime::GetMonthName($arg),
        rweekofyear:        DateTime::GetWeekOfYear($arg),
        rweekofyeariso8601: DateTime::GetWeekOfYearIso8601($arg),
        rdayofmonth:        DateTime::GetDayOfMonth($arg),
        rdayofweek:         DateTime::GetDayOfWeek($arg),
        rdayofweekname:     DateTime::GetDayOfWeekName($arg),
        rhour:              DateTime::GetHour($arg),
        rminute:            DateTime::GetMinute($arg),
        rsecond:            DateTime::GetSecond($arg),
        rmsec:              DateTime::GetMillisecondOfSecond($arg),
        rusec:              DateTime::GetMicrosecondOfSecond($arg),
        rtz:                DateTime::GetTimezoneId($arg),
        rtzname:            DateTime::GetTimezoneName($arg),
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
