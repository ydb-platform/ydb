/* syntax version 1 */
select
cast(DateTime::MakeDate(TzDatetime("2000-01-01T12:00:00,Europe/Moscow") ) as String),
cast(DateTime::MakeTzDate(TzDatetime("2000-01-01T12:00:00,Europe/Moscow") ) as String),

cast(DateTime::MakeDate(TzDatetime("2000-01-01T00:00:00,Europe/Moscow") ) as String),
cast(DateTime::MakeTzDate(TzDatetime("2000-01-01T00:00:00,Europe/Moscow") ) as String);
