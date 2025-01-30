/* syntax version 1 */
/* postgres can not */
SELECT
    CurrentTzDate('Europe/Moscow'),
    CurrentTzDatetime('Europe/Moscow'),
    CurrentTzTimestamp('Europe/Moscow'),
    CurrentUtcDate(),
    CurrentUtcDatetime(),
    CurrentUtcTimestamp()
;
