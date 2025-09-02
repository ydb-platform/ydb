/* postgres can not */
/* syntax version 1 */
select ToBytes(TzDate32("1901-01-01,Europe/Moscow"));
select cast(FromBytes(ToBytes(TzDate32("1901-01-01,Europe/Moscow")),TzDate32) as string);

select ToBytes(TzDatetime64("1901-01-01T01:02:03,Europe/Moscow"));
select cast(FromBytes(ToBytes(TzDatetime64("1901-01-01T01:02:03,Europe/Moscow")),TzDatetime64) as string);

select ToBytes(TzTimestamp64("1901-01-01T01:02:03.456789,Europe/Moscow"));
select cast(FromBytes(ToBytes(TzTimestamp64("1901-01-01T01:02:03.456789,Europe/Moscow")),TzTimestamp64) as string);

