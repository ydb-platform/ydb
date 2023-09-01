/* syntax version 1 */
pragma UseBlocks;
insert into @t
    select
        cast(fdate as Date) as `date`,
        cast(fdatetime as Datetime) as `datetime`,
        cast(ftimestamp as Timestamp) as `timestamp`,
    from Input;

commit;
SELECT
    DateTime::GetHour(`date`) as date_hour,
    DateTime::GetMinute(`date`) as date_minute,
    DateTime::GetSecond(`date`) as date_second,
    DateTime::GetMillisecondOfSecond(`date`) as date_milli,
    DateTime::GetMicrosecondOfSecond(`date`) as date_micro,

    DateTime::GetHour(`datetime`) as datetime_hour,
    DateTime::GetMinute(`datetime`) as datetime_minute,
    DateTime::GetSecond(`datetime`) as datetime_second,
    DateTime::GetMillisecondOfSecond(`datetime`) as datetime_milli,
    DateTime::GetMicrosecondOfSecond(`datetime`) as datetime_micro,

    DateTime::GetHour(`timestamp`) as timestamp_hour,
    DateTime::GetMinute(`timestamp`) as timestamp_minute,
    DateTime::GetSecond(`timestamp`) as timestamp_second,
    DateTime::GetMillisecondOfSecond(`timestamp`) as timestamp_milli,
    DateTime::GetMicrosecondOfSecond(`timestamp`) as timestamp_micro,
FROM @t;

