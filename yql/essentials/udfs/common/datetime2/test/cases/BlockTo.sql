/* syntax version 1 */
pragma UseBlocks;
insert into @t
    select
        cast(fdate as Date) as `date`,
        cast(fdatetime as Datetime) as `datetime`,
        cast(ftimestamp as Timestamp) as `timestamp`,
        cast(finterval as Interval) as `interval`,
        cast(ftzdate as TzDate) as `tzdate`,
        cast(ftzdatetime as TzDatetime) as `tzdatetime`,
        cast(ftztimestamp as TzTimestamp) as `tztimestamp`,
        cast(null as Interval) as `interval_null`,
        -cast(finterval_1day as Interval) as `negative_1d`,
    from Input;

commit;
select
    DateTime::ToDays(`interval`) as interval_to_days,
    DateTime::ToHours(`interval`) as interval_to_hours,
    DateTime::ToMinutes(`interval`) as interval_to_minutes,
    DateTime::ToSeconds(`interval`) as interval_to_seconds,
    DateTime::ToMilliseconds(`interval`) as interval_to_msec,
    DateTime::ToMicroseconds(`interval`) as interval_to_usec,

    DateTime::ToSeconds(`date`) as date_to_seconds,
    DateTime::ToSeconds(`datetime`) as datetime_to_seconds,
    DateTime::ToSeconds(`timestamp`) as timestamp_to_seconds,
    DateTime::ToSeconds(`tzdate`) as tzdate_to_seconds,
    DateTime::ToSeconds(`tzdatetime`) as tzdatetime_to_seconds,
    DateTime::ToSeconds(`tztimestamp`) as tztimestamp_to_seconds,

    DateTime::ToMilliseconds(`date`) as date_to_msec,
    DateTime::ToMilliseconds(`datetime`) as datetime_to_msec,
    DateTime::ToMilliseconds(`timestamp`) as timestamp_to_msec,
    DateTime::ToMilliseconds(`tzdate`) as tzdate_to_msec,
    DateTime::ToMilliseconds(`tzdatetime`) as tzdatetime_to_msec,
    DateTime::ToMilliseconds(`tztimestamp`) as tztimestamp_to_msec,

    DateTime::ToMicroseconds(`date`) as date_to_usec,
    DateTime::ToMicroseconds(`datetime`) as datetime_to_usec,
    DateTime::ToMicroseconds(`timestamp`) as timestamp_to_usec,
    DateTime::ToMicroseconds(`tzdate`) as tzdate_to_usec,
    DateTime::ToMicroseconds(`tzdatetime`) as tzdatetime_to_usec,
    DateTime::ToMicroseconds(`tztimestamp`) as tztimestamp_to_usec,

    DateTime::ToDays(`interval_null`) as interval_null,

    /* Overflow test */
    DateTime::ToDays(`negative_1d`) as negative_1d,
from @t;

