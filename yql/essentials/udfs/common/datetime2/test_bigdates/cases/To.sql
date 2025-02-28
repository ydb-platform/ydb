/* syntax version 1 */
SELECT
    DateTime::ToDays(finterval64) as interval64_to_days,
    DateTime::ToHours(finterval64) as interval64_to_hours,
    DateTime::ToMinutes(finterval64) as interval64_to_minutes,
    DateTime::ToSeconds(finterval64) as interval64_to_seconds,
    DateTime::ToMilliseconds(finterval64) as interval64_to_msec,
    DateTime::ToMicroseconds(finterval64) as interval64_to_usec,

    DateTime::ToSeconds(fdate32) as date32_to_seconds,
    DateTime::ToSeconds(fdatetime64) as datetime64_to_seconds,
    DateTime::ToSeconds(ftimestamp64) as timestamp64_to_seconds,
    DateTime::ToSeconds(ftzdate32) as tzdate32_to_seconds,
    DateTime::ToSeconds(ftzdatetime64) as tzdatetime64_to_seconds,
    DateTime::ToSeconds(ftztimestamp64) as tztimestamp64_to_seconds,

    DateTime::ToMilliseconds(fdate32) as date32_to_msec,
    DateTime::ToMilliseconds(fdatetime64) as datetime64_to_msec,
    DateTime::ToMilliseconds(ftimestamp64) as timestamp64_to_msec,
    DateTime::ToMilliseconds(ftzdate32) as tzdate32_to_msec,
    DateTime::ToMilliseconds(ftzdatetime64) as tzdatetime64_to_msec,
    DateTime::ToMilliseconds(ftztimestamp64) as tztimestamp64_to_msec,

    DateTime::ToMicroseconds(fdate32) as date32_to_usec,
    DateTime::ToMicroseconds(fdatetime64) as datetime64_to_usec,
    DateTime::ToMicroseconds(ftimestamp64) as timestamp64_to_usec,
    DateTime::ToMicroseconds(ftzdate32) as tzdate32_to_usec,
    DateTime::ToMicroseconds(ftzdatetime64) as tzdatetime64_to_usec,
    DateTime::ToMicroseconds(ftztimestamp64) as tztimestamp64_to_usec,
FROM (
    SELECT
        CAST(fdate32 as Date32) as fdate32,
        CAST(fdatetime64 as Datetime64) as fdatetime64,
        CAST(ftimestamp64 as Timestamp64) as ftimestamp64,
        CAST(finterval64 as Interval64) as finterval64,
        CAST(ftzdate32 as TzDate32) as ftzdate32,
        CAST(ftzdatetime64 as TzDatetime64) as ftzdatetime64,
        CAST(ftztimestamp64 as TzTimestamp64) as ftztimestamp64,
    from Input
);
