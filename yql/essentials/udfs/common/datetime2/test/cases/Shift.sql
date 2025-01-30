SELECT
    cast(DateTime::MakeTzTimestamp(DateTime::ShiftYears(tm, 10)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::ShiftYears(tm, 10)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::ShiftQuarters(tm, 16)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::ShiftQuarters(tm, -16)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::ShiftMonths(tm, 0)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::ShiftMonths(tm, 1)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::ShiftMonths(tm, 3)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::ShiftMonths(tm, 11)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::ShiftMonths(tm, 12)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::ShiftMonths(tm, 123)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::ShiftMonths(tm, -1)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::ShiftMonths(tm, -3)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::ShiftMonths(tm, -11)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::ShiftMonths(tm, -12)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::ShiftMonths(tm, -123)) as String)
from (
    select
        cast(ftztimestamp as TzTimestamp) as tm
    from Input
);

