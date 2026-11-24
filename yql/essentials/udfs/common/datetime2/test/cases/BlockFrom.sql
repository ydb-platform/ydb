$rows = [
    <|fts_seconds: 3875345u, fts_msec: 3875345000ul, fts_usec: 3875345000000ul, fdays: 7s, fdays_overflow: 100000, fdays_null: Nothing(Optional<Int16>), fhours: 168, fminutes: 10080, fseconds: 604800, fmsec: 604800000l, fusec: -604800000000l|>,
    <|fts_seconds: 3875345u, fts_msec: 3875345000ul, fts_usec: 3875345000000ul, fdays: 7s, fdays_overflow: 100000, fdays_null: Nothing(Optional<Int16>), fhours: 168, fminutes: 10080, fseconds: 604800, fmsec: 604800000l, fusec: -604800000000l|>,
];

pragma UseBlocks;

select
    DateTime::FromSeconds(fts_seconds) as ts_seconds,
    DateTime::FromMilliseconds(fts_msec) as ts_msec,
    DateTime::FromMicroseconds(fts_usec) as ts_usec,
    DateTime::FromMicroseconds(fts_msec * fts_msec) as ts_empty,

    DateTime::IntervalFromDays(fdays) as interval_days,
    DateTime::IntervalFromHours(fhours) as interval_hours,
    DateTime::IntervalFromMinutes(fminutes) as interval_minutes,
    DateTime::IntervalFromSeconds(fseconds) as interval_seconds,
    DateTime::IntervalFromMilliseconds(fmsec) as interval_msec,
    DateTime::IntervalFromMicroseconds(fusec) as interval_usec,

    DateTime::IntervalFromDays(fdays_overflow) as interval_days_overflow,
    DateTime::IntervalFromDays(fdays_null) as interval_null,
FROM AS_TABLE($rows)
