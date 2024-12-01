/* postgres can not */
select cast(TzDate("2000-01-01,Europe/Moscow") as TzDatetime);
select cast(TzDate("2000-01-01,Europe/Moscow") as TzTimestamp);
select cast(TzDatetime("2000-01-01T12:00:00,Europe/Moscow") as TzDate);
select cast(TzDatetime("2000-01-01T12:00:00,Europe/Moscow") as TzTimestamp);
select cast(TzTimestamp("2000-01-01T12:00:00.456789,Europe/Moscow") as TzDate);
select cast(TzTimestamp("2000-01-01T12:00:00.456789,Europe/Moscow") as TzDatetime);

select cast(TzDate("2000-01-01,America/Los_Angeles") as TzDatetime);
select cast(TzDate("2000-01-01,America/Los_Angeles") as TzTimestamp);
select cast(TzDatetime("2000-01-01T12:00:00,America/Los_Angeles") as TzDate);
select cast(TzDatetime("2000-01-01T12:00:00,America/Los_Angeles") as TzTimestamp);
select cast(TzTimestamp("2000-01-01T12:00:00.456789,America/Los_Angeles") as TzDate);
select cast(TzTimestamp("2000-01-01T12:00:00.456789,America/Los_Angeles") as TzDatetime);
