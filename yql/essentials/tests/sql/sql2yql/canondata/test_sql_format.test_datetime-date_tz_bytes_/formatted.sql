/* postgres can not */
/* syntax version 1 */
SELECT
    ToBytes(TzDate('2001-01-01,Europe/Moscow'))
;

SELECT
    CAST(FromBytes(ToBytes(TzDate('2001-01-01,Europe/Moscow')), TzDate) AS string)
;

SELECT
    ToBytes(TzDatetime('2001-01-01T01:02:03,Europe/Moscow'))
;

SELECT
    CAST(FromBytes(ToBytes(TzDatetime('2001-01-01T01:02:03,Europe/Moscow')), TzDatetime) AS string)
;

SELECT
    ToBytes(TzTimestamp('2001-01-01T01:02:03.456789,Europe/Moscow'))
;

SELECT
    CAST(FromBytes(ToBytes(TzTimestamp('2001-01-01T01:02:03.456789,Europe/Moscow')), TzTimestamp) AS string)
;
