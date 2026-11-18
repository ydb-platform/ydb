$rows = [
    <|fdate32: Just(Date32("-144169-01-01")), fdatetime64: Just(Datetime64("-144169-01-01T00:00:00Z")), ftimestamp64: Just(Timestamp64("-144169-01-01T00:00:00.000000Z")), ftzdate32: Just(TzDate32("-144169-01-02,Europe/Moscow")), ftzdatetime64: Just(TzDatetime64("-144169-01-01T02:30:17,Europe/Moscow")), ftztimestamp64: Just(TzTimestamp64("-144169-01-01T02:30:17.000000,Europe/Moscow")), finterval64: Just(Interval64("-PT9223339708799S"))|>,
    <|fdate32: Just(Date32("-1-01-01")), fdatetime64: Just(Datetime64("-1-01-01T23:59:59Z")), ftimestamp64: Just(Timestamp64("-1-01-01T23:59:59.999999Z")), ftzdate32: Just(TzDate32("-1-01-01,Europe/Moscow")), ftzdatetime64: Just(TzDatetime64("-1-01-01T23:59:59,Europe/Moscow")), ftztimestamp64: Just(TzTimestamp64("-1-01-01T23:59:59.999999,Europe/Moscow")), finterval64: Just(Interval64("-PT4611669897600S"))|>,
    <|fdate32: Just(Date32("1-01-01")), fdatetime64: Just(Datetime64("1-01-01T00:00:00Z")), ftimestamp64: Just(Timestamp64("1-01-01T00:00:00.000000Z")), ftzdate32: Just(TzDate32("1-01-01,Europe/Moscow")), ftzdatetime64: Just(TzDatetime64("1-01-01T00:00:00,Europe/Moscow")), ftztimestamp64: Just(TzTimestamp64("1-01-01T00:00:00.000000,Europe/Moscow")), finterval64: Just(Interval64("-P1D"))|>,
    <|fdate32: Just(Date32("1969-12-31")), fdatetime64: Just(Datetime64("1969-12-31T23:59:59Z")), ftimestamp64: Just(Timestamp64("1969-12-31T23:59:59.999999Z")), ftzdate32: Just(TzDate32("1969-12-31,Europe/Moscow")), ftzdatetime64: Just(TzDatetime64("1969-12-31T23:59:59,Europe/Moscow")), ftztimestamp64: Just(TzTimestamp64("1969-12-31T23:59:59.999999,Europe/Moscow")), finterval64: Just(Interval64("P1W1DT1H1M1.000001S"))|>,
    <|fdate32: Just(Date32("1970-01-01")), fdatetime64: Just(Datetime64("1970-01-01T00:00:00Z")), ftimestamp64: Just(Timestamp64("1970-01-01T00:00:00.000000Z")), ftzdate32: Just(TzDate32("1970-01-01,Europe/Moscow")), ftzdatetime64: Just(TzDatetime64("1970-01-01T00:00:00,Europe/Moscow")), ftztimestamp64: Just(TzTimestamp64("1970-01-01T00:00:00.000000,Europe/Moscow")), finterval64: Just(Interval64("P1D"))|>,
    <|fdate32: Just(Date32("2025-01-01")), fdatetime64: Just(Datetime64("2025-01-01T01:02:03Z")), ftimestamp64: Just(Timestamp64("2025-01-01T01:02:03.456789Z")), ftzdate32: Just(TzDate32("2025-01-01,Europe/Moscow")), ftzdatetime64: Just(TzDatetime64("2025-01-01T04:02:03,Europe/Moscow")), ftztimestamp64: Just(TzTimestamp64("2025-01-01T04:02:03.456789,Europe/Moscow")), finterval64: Just(Interval64("P1W2DT3H4M5.678912S"))|>,
    <|fdate32: Just(Date32("2106-01-01")), fdatetime64: Just(Datetime64("2106-01-01T00:00:00Z")), ftimestamp64: Just(Timestamp64("2106-01-01T00:00:00.000000Z")), ftzdate32: Just(TzDate32("2106-01-01,Europe/Moscow")), ftzdatetime64: Just(TzDatetime64("2106-01-01T03:00:00,Europe/Moscow")), ftztimestamp64: Just(TzTimestamp64("2106-01-01T03:00:00.000000,Europe/Moscow")), finterval64: Just(Interval64("PT4611669811199S"))|>,
    <|fdate32: Just(Date32("148107-12-31")), fdatetime64: Just(Datetime64("148107-12-31T23:59:59Z")), ftimestamp64: Just(Timestamp64("148107-12-31T23:59:59.999999Z")), ftzdate32: Just(TzDate32("148107-12-31,Europe/Moscow")), ftzdatetime64: Just(TzDatetime64("148108-01-01T02:59:59,Europe/Moscow")), ftztimestamp64: Just(TzTimestamp64("148108-01-01T02:59:59.999999,Europe/Moscow")), finterval64: Just(Interval64("PT9223339708799S"))|>,
];

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
FROM AS_TABLE($rows);
