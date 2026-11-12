$rows = [
    <|fdate: Just(Date("2018-11-22")), fdatetime: Just(Datetime("2018-11-22T21:20:19Z")), ftimestamp: Just(Timestamp("2018-11-22T21:20:19.345678Z")), finterval: Just(Interval("P2DT10H")), ftzdate: Just(TzDate("2018-11-22,Europe/Moscow")), ftzdatetime: Just(TzDatetime("2018-11-22T21:20:19,Europe/Moscow")), ftztimestamp: Just(TzTimestamp("2018-11-22T21:20:19.345678,Europe/Moscow")), finterval_1day: Just(Interval("P1D"))|>,
];

select
    DateTime::ToDays(finterval) as interval_to_days,
    DateTime::ToHours(finterval) as interval_to_hours,
    DateTime::ToMinutes(finterval) as interval_to_minutes,
    DateTime::ToSeconds(finterval) as interval_to_seconds,
    DateTime::ToMilliseconds(finterval) as interval_to_msec,
    DateTime::ToMicroseconds(finterval) as interval_to_usec,

    DateTime::ToSeconds(fdate) as date_to_seconds,
    DateTime::ToSeconds(fdatetime) as datetime_to_seconds,
    DateTime::ToSeconds(ftimestamp) as timestamp_to_seconds,
    DateTime::ToSeconds(ftzdate) as tzdate_to_seconds,
    DateTime::ToSeconds(ftzdatetime) as tzdatetime_to_seconds,
    DateTime::ToSeconds(ftztimestamp) as tztimestamp_to_seconds,

    DateTime::ToMilliseconds(fdate) as date_to_msec,
    DateTime::ToMilliseconds(fdatetime) as datetime_to_msec,
    DateTime::ToMilliseconds(ftimestamp) as timestamp_to_msec,
    DateTime::ToMilliseconds(ftzdate) as tzdate_to_msec,
    DateTime::ToMilliseconds(ftzdatetime) as tzdatetime_to_msec,
    DateTime::ToMilliseconds(ftztimestamp) as tztimestamp_to_msec,

    DateTime::ToMicroseconds(fdate) as date_to_usec,
    DateTime::ToMicroseconds(fdatetime) as datetime_to_usec,
    DateTime::ToMicroseconds(ftimestamp) as timestamp_to_usec,
    DateTime::ToMicroseconds(ftzdate) as tzdate_to_usec,
    DateTime::ToMicroseconds(ftzdatetime) as tzdatetime_to_usec,
    DateTime::ToMicroseconds(ftztimestamp) as tztimestamp_to_usec,

    /* Overflow test */
    DateTime::ToDays(-finterval_1day) as negative_1d,
FROM AS_TABLE($rows);
