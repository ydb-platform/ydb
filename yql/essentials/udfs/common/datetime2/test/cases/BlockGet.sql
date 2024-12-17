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
    DateTime::GetHour(`date`) as date_hour,
    DateTime::GetMinute(`date`) as date_minute,
    DateTime::GetSecond(`date`) as date_second,
    DateTime::GetMillisecondOfSecond(`date`) as date_msec,
    DateTime::GetMicrosecondOfSecond(`date`) as date_usec,
    DateTime::GetTimezoneId(`date`) as date_tz,
    DateTime::GetTimezoneName(`date`) as date_tzname,

    DateTime::GetHour(`datetime`) as datetime_hour,
    DateTime::GetMinute(`datetime`) as datetime_minute,
    DateTime::GetSecond(`datetime`) as datetime_second,
    DateTime::GetMillisecondOfSecond(`datetime`) as datetime_msec,
    DateTime::GetMicrosecondOfSecond(`datetime`) as datetime_usec,
    DateTime::GetTimezoneId(`datetime`) as datetime_tz,
    DateTime::GetTimezoneName(`datetime`) as datetime_tzname,

    DateTime::GetHour(`timestamp`) as timestamp_hour,
    DateTime::GetMinute(`timestamp`) as timestamp_minute,
    DateTime::GetSecond(`timestamp`) as timestamp_second,
    DateTime::GetMillisecondOfSecond(`timestamp`) as timestamp_msec,
    DateTime::GetMicrosecondOfSecond(`timestamp`) as timestamp_usec,
    DateTime::GetTimezoneId(`timestamp`) as timestamp_tz,
    DateTime::GetTimezoneName(`timestamp`) as timestamp_tzname,
FROM @t;

