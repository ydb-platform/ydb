$rows = [
    <|tm: Just(TzTimestamp("1970-01-01T11:14:00.000000,GMT"))|>,
    <|tm: Just(TzTimestamp("1970-01-02T14:08:00.000000,GMT"))|>,
    <|tm: Just(TzTimestamp("1970-02-01T17:03:00.000000,GMT"))|>,
    <|tm: Just(TzTimestamp("1970-09-03T07:22:00.000000,GMT"))|>,
    <|tm: Just(TzTimestamp("1970-12-31T23:59:59.999999,GMT"))|>,
    <|tm: Just(TzTimestamp("1971-01-01T00:00:00.000000,GMT"))|>,
    <|tm: Just(TzTimestamp("1971-01-14T00:00:00.000000,GMT"))|>,
    <|tm: Just(TzTimestamp("1978-01-25T16:15:00.000000,Europe/Uzhgorod"))|>,
    <|tm: Just(TzTimestamp("2018-12-01T01:02:03.456789,Europe/Moscow"))|>,
];

pragma UseBlocks;

SELECT
    DateTime::GetYear(tm) as ryear,
    DateTime::GetDayOfYear(tm) as rdayofyear,
    DateTime::GetMonth(tm) as rmonth,
    DateTime::GetMonthName(tm) as rmonthname,
    DateTime::GetWeekOfYear(tm) as rweekofyear,
    DateTime::GetWeekOfYearIso8601(tm) as rweekofyeariso8601,
    DateTime::GetDayOfMonth(tm) as rdayofmonth,
    DateTime::GetDayOfWeek(tm) as rdayofweek,
    DateTime::GetDayOfWeekName(tm) as rdayofweekname,
    DateTime::GetHour(tm) as rhour,
    DateTime::GetMinute(tm) as rminute,
    DateTime::GetSecond(tm) as rsecond,
    DateTime::GetMillisecondOfSecond(tm) as rmsec,
    DateTime::GetMicrosecondOfSecond(tm) as rusec,
    DateTime::GetTimezoneId(tm) as rtz,
    DateTime::GetTimezoneName(tm) as rtzname
FROM AS_TABLE($rows);
