$rows = [
    <|fdate: Date("2018-11-22"), fdatetime: Datetime("2018-11-22T21:20:19Z"), ftimestamp: Timestamp("2018-11-22T21:20:19.345678Z")|>,
];

pragma UseBlocks;

SELECT
    DateTime::GetHour(fdate) as date_hour,
    DateTime::GetMinute(fdate) as date_minute,
    DateTime::GetSecond(fdate) as date_second,
    DateTime::GetMillisecondOfSecond(fdate) as date_msec,
    DateTime::GetMicrosecondOfSecond(fdate) as date_usec,
    DateTime::GetTimezoneId(fdate) as date_tz,
    DateTime::GetTimezoneName(fdate) as date_tzname,

    DateTime::GetHour(fdatetime) as datetime_hour,
    DateTime::GetMinute(fdatetime) as datetime_minute,
    DateTime::GetSecond(fdatetime) as datetime_second,
    DateTime::GetMillisecondOfSecond(fdatetime) as datetime_msec,
    DateTime::GetMicrosecondOfSecond(fdatetime) as datetime_usec,
    DateTime::GetTimezoneId(fdatetime) as datetime_tz,
    DateTime::GetTimezoneName(fdatetime) as datetime_tzname,

    DateTime::GetHour(ftimestamp) as timestamp_hour,
    DateTime::GetMinute(ftimestamp) as timestamp_minute,
    DateTime::GetSecond(ftimestamp) as timestamp_second,
    DateTime::GetMillisecondOfSecond(ftimestamp) as timestamp_msec,
    DateTime::GetMicrosecondOfSecond(ftimestamp) as timestamp_usec,
    DateTime::GetTimezoneId(ftimestamp) as timestamp_tz,
    DateTime::GetTimezoneName(ftimestamp) as timestamp_tzname,
FROM AS_TABLE($rows);
