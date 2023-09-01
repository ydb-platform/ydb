/* syntax version 0 */
SELECT
    cast(DateTime::TimestampFromString(value) as string) as ts,
    cast(DateTime::DateStartOfDay(cast(DateTime::TimestampFromString(value) as date)) as string) as start_d, 
    cast(DateTime::DatetimeStartOfDay(cast(DateTime::TimestampFromString(value) as datetime)) as string) as start_dt,
    cast(DateTime::TimestampStartOfDay(DateTime::TimestampFromString(value)) as string) as start_ts,
    cast(DateTime::GetTimeOfDay(DateTime::TimestampFromString(value)) as string) as day_time
FROM Input;
