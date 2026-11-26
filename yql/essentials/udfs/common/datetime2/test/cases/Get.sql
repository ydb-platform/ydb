$rows = [
    <|ftztimestamp: Just(TzTimestamp("1970-01-01T11:14:00.000000,GMT"))|>,
    <|ftztimestamp: Just(TzTimestamp("1970-01-02T14:08:00.000000,GMT"))|>,
    <|ftztimestamp: Just(TzTimestamp("1970-02-01T17:03:00.000000,GMT"))|>,
    <|ftztimestamp: Just(TzTimestamp("1970-09-03T07:22:00.000000,GMT"))|>,
    <|ftztimestamp: Just(TzTimestamp("1970-12-31T23:59:59.999999,GMT"))|>,
    <|ftztimestamp: Just(TzTimestamp("1971-01-01T00:00:00.000000,GMT"))|>,
    <|ftztimestamp: Just(TzTimestamp("1971-01-14T00:00:00.000000,GMT"))|>,
    <|ftztimestamp: Just(TzTimestamp("1978-01-25T16:15:00.000000,Europe/Uzhgorod"))|>,
    <|ftztimestamp: Just(TzTimestamp("2018-12-01T01:02:03.456789,Europe/Moscow"))|>,
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
    $tm = $row.ftztimestamp;
    return <|
        explicit: $check(DateTime::Split($tm)),
        implicit: $check($tm),
    |>;
};

PROCESS AS_TABLE($rows) USING $typeDispatcher(TableRow());
