/* syntax version 1 */
pragma UseBlocks;
insert into @t
    select
        Unwrap(cast(fdate as Date)) as `date`,
        Unwrap(cast(fdatetime as Datetime)) as `datetime`,
        Unwrap(cast(ftimestamp as Timestamp)) as `timestamp`,
    from Input;
commit;

SELECT
    DateTime::GetYear(`date`) as date_year,
    DateTime::GetDayOfYear(`date`) as date_dayofyear,
    DateTime::GetMonth(`date`) as date_month,
    DateTime::GetMonthName(`date`) as date_monthname,
    DateTime::GetWeekOfYear(`date`) as date_weekofyear,
    DateTime::GetWeekOfYearIso8601(`date`) as date_weekofyeariso8601,
    DateTime::GetDayOfMonth(`date`) as date_dayofmonth,
    DateTime::GetDayOfWeek(`date`) as date_dayofweek,
    DateTime::GetDayOfWeekName(`date`) as date_dayofweekname,
    DateTime::GetHour(`date`) as date_hour,
    DateTime::GetMinute(`date`) as date_minute,
    DateTime::GetSecond(`date`) as date_second,
    DateTime::GetMillisecondOfSecond(`date`) as date_msec,
    DateTime::GetMicrosecondOfSecond(`date`) as date_usec,
    DateTime::GetTimezoneId(`date`) as date_tz,
    DateTime::GetTimezoneName(`date`) as date_tzname,

    DateTime::GetYear(`datetime`) as datetime_year,
    DateTime::GetDayOfYear(`datetime`) as datetime_dayofyear,
    DateTime::GetMonth(`datetime`) as datetime_month,
    DateTime::GetMonthName(`datetime`) as datetime_monthname,
    DateTime::GetWeekOfYear(`datetime`) as datetime_weekofyear,
    DateTime::GetWeekOfYearIso8601(`datetime`) as datetime_weekofyeariso8601,
    DateTime::GetDayOfMonth(`datetime`) as datetime_dayofmonth,
    DateTime::GetDayOfWeek(`datetime`) as datetime_dayofweek,
    DateTime::GetDayOfWeekName(`datetime`) as datetime_dayofweekname,
    DateTime::GetHour(`datetime`) as datetime_hour,
    DateTime::GetMinute(`datetime`) as datetime_minute,
    DateTime::GetSecond(`datetime`) as datetime_second,
    DateTime::GetMillisecondOfSecond(`datetime`) as datetime_msec,
    DateTime::GetMicrosecondOfSecond(`datetime`) as datetime_usec,
    DateTime::GetTimezoneId(`datetime`) as datetime_tz,
    DateTime::GetTimezoneName(`datetime`) as datetime_tzname,

    DateTime::GetYear(`timestamp`) as timestamp_year,
    DateTime::GetDayOfYear(`timestamp`) as timestamp_dayofyear,
    DateTime::GetMonth(`timestamp`) as timestamp_month,
    DateTime::GetMonthName(`timestamp`) as timestamp_monthname,
    DateTime::GetWeekOfYear(`timestamp`) as timestamp_weekofyear,
    DateTime::GetWeekOfYearIso8601(`timestamp`) as timestamp_weekofyeariso8601,
    DateTime::GetDayOfMonth(`timestamp`) as timestamp_dayofmonth,
    DateTime::GetDayOfWeek(`timestamp`) as timestamp_dayofweek,
    DateTime::GetDayOfWeekName(`timestamp`) as timestamp_dayofweekname,
    DateTime::GetHour(`timestamp`) as timestamp_hour,
    DateTime::GetMinute(`timestamp`) as timestamp_minute,
    DateTime::GetSecond(`timestamp`) as timestamp_second,
    DateTime::GetMillisecondOfSecond(`timestamp`) as timestamp_msec,
    DateTime::GetMicrosecondOfSecond(`timestamp`) as timestamp_usec,
    DateTime::GetTimezoneId(`timestamp`) as timestamp_tz,
    DateTime::GetTimezoneName(`timestamp`) as timestamp_tzname,
FROM @t;

