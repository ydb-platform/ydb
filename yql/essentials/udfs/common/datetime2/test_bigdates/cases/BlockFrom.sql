$rows = [
    <|fts64_sec: 3875345l, fts64_msec: 3875345000l, fts64_usec: 3875345000000l, fi64_days: 7, fi64_hours: 168l, fi64_minutes: 10080l, fi64_seconds: 604800l, fi64_msec: 604800000l, fi64_usec: 604800000000l|>,
    <|fts64_sec: 4291747200l, fts64_msec: 4291747200000l, fts64_usec: 4291747200000000l, fi64_days: 49673, fi64_hours: 1192152l, fi64_minutes: 71529120l, fi64_seconds: 4291747200l, fi64_msec: 4291747200000l, fi64_usec: 4291747200000000l|>,
    <|fts64_sec: -4611669897600l, fts64_msec: -4611669897600000l, fts64_usec: -4611669897600000000l, fi64_days: -106751616, fi64_hours: -2562038784l, fi64_minutes: -153722327040l, fi64_seconds: -9223339708799l, fi64_msec: -9223339708799999l, fi64_usec: -9223339708799999999l|>,
    <|fts64_sec: -4611669897601l, fts64_msec: -4611669897600001l, fts64_usec: -4611669897600000001l, fi64_days: -106751617, fi64_hours: -2562038808l, fi64_minutes: -153722328480l, fi64_seconds: -9223339708800l, fi64_msec: -9223339708800000l, fi64_usec: -9223339708800000000l|>,
    <|fts64_sec: 4611669811199l, fts64_msec: 4611669811199999l, fts64_usec: 4611669811199999999l, fi64_days: 106751616, fi64_hours: 2562038784l, fi64_minutes: 153722327040l, fi64_seconds: 9223339708799l, fi64_msec: 9223339708799999l, fi64_usec: 9223339708799999999l|>,
    <|fts64_sec: 4611669811200l, fts64_msec: 4611669811200000l, fts64_usec: 4611669811200000000l, fi64_days: 106751617, fi64_hours: 2562038808l, fi64_minutes: 153722328480l, fi64_seconds: 9223339708800l, fi64_msec: 9223339708800000l, fi64_usec: 9223339708800000000l|>,
];

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
FROM AS_TABLE($rows)
