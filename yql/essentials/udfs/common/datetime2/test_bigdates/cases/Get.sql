$rows = [
    <|ftztimestamp64: Just(TzTimestamp64("-144169-01-01T00:00:00.000000,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("-1-01-01T23:59:59.999999,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("1-01-01T00:00:00.000000,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("1969-12-31T23:59:59.999999,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("1970-01-01T00:00:00.000000,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("2025-01-01T00:00:00.000000,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("2106-01-01T00:00:00.000000,GMT"))|>,
    <|ftztimestamp64: Just(TzTimestamp64("148107-12-31T23:59:59.999999,GMT"))|>,
];

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
    $tm = $row.ftztimestamp64;
    return <|
        explicit: $check(DateTime::Split($tm)),
        implicit: $check($tm),
    |>;
};

PROCESS AS_TABLE($rows) USING $typeDispatcher(TableRow());
