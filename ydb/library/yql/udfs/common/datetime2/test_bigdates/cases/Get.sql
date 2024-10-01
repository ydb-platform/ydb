/* syntax version 1 */
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
FROM (
    SELECT
        DateTime::Split(CAST(ftimestamp64 as Timestamp64)) as tm
    FROM Input
);
