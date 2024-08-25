/* syntax version 1 */
pragma UseBlocks;
insert into @t
    select
        cast(ftztimestamp as TzTimestamp) as `tm`,
    from Input;

commit;

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
FROM @t;
