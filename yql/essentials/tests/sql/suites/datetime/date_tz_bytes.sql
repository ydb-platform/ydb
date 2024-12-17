/* postgres can not */
/* syntax version 1 */
select ToBytes(TzDate("2001-01-01,Europe/Moscow"));
select cast(FromBytes(ToBytes(TzDate("2001-01-01,Europe/Moscow")),TzDate) as string);

select ToBytes(TzDatetime("2001-01-01T01:02:03,Europe/Moscow"));
select cast(FromBytes(ToBytes(TzDatetime("2001-01-01T01:02:03,Europe/Moscow")),TzDatetime) as string);

select ToBytes(TzTimestamp("2001-01-01T01:02:03.456789,Europe/Moscow"));
select cast(FromBytes(ToBytes(TzTimestamp("2001-01-01T01:02:03.456789,Europe/Moscow")),TzTimestamp) as string);
