/* syntax version 1 */
SELECT
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, 2005)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, 2200 as Year)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, NULL, 7)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, 13 as Month)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, NULL, NULL, 20)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, 32 as Day)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, 2018, 2, 30)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, NULL, NULL, NULL, 11, 10, 9)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, 11 as Hour)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, 24 as Hour)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, 10 as Minute)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, 60 as Minute)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, 9 as Second)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, 60 as Second)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, 123456 as Microsecond)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, 2000000 as Microsecond)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, 100 as TimezoneId)) as String),
    cast(DateTime::MakeTzTimestamp(DateTime::Update(tm, 1000 as TimezoneId)) as String)
from (
    select
        cast(ftztimestamp as TzTimestamp) as tm
    from Input
);
