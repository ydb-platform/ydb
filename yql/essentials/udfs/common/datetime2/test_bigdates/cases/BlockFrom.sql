/* syntax version 1 */
pragma UseBlocks;

select
    DateTime::FromSeconds64(fts64_sec) as ts64_sec,
    DateTime::FromMilliseconds64(fts64_msec) as ts64_msec,
    DateTime::FromMicroseconds64(fts64_usec) as ts64_usec,

    DateTime::Interval64FromDays(fi64_days) as interval64_days,
    DateTime::Interval64FromHours(fi64_hours) as interval64_hours,
    DateTime::Interval64FromMinutes(fi64_minutes) as interval64_minutes,
    DateTime::Interval64FromSeconds(fi64_seconds) as interval64_seconds,
    DateTime::Interval64FromMilliseconds(fi64_msec) as interval64_msec,
    DateTime::Interval64FromMicroseconds(fi64_usec) as interval64_usec
from Input
