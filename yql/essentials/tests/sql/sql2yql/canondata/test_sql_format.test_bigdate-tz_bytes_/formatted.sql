/* postgres can not */
/* syntax version 1 */
SELECT
    ToBytes(TzDate32('1901-01-01,Europe/Moscow'))
;

SELECT
    CAST(FromBytes(ToBytes(TzDate32('1901-01-01,Europe/Moscow')), TzDate32) AS string)
;

SELECT
    ToBytes(TzDatetime64('1901-01-01T01:02:03,Europe/Moscow'))
;

SELECT
    CAST(FromBytes(ToBytes(TzDatetime64('1901-01-01T01:02:03,Europe/Moscow')), TzDatetime64) AS string)
;

SELECT
    ToBytes(TzTimestamp64('1901-01-01T01:02:03.456789,Europe/Moscow'))
;

SELECT
    CAST(FromBytes(ToBytes(TzTimestamp64('1901-01-01T01:02:03.456789,Europe/Moscow')), TzTimestamp64) AS string)
;
