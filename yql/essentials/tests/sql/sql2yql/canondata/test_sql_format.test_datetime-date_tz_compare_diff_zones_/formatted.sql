/* postgres can not */
SELECT
    CAST('2000-01-01,GMT' AS tzdate) > tzdate('2000-01-01,Europe/Moscow')
;

SELECT
    CAST('1999-12-31,GMT' AS tzdate) == tzdate('2000-01-01,Europe/Moscow')
;

SELECT
    RemoveTimezone(CAST('1999-12-31,GMT' AS tzdate)) == RemoveTimezone(tzdate('2000-01-01,Europe/Moscow'))
;

SELECT
    CAST('2000-01-01,GMT' AS tzdate) == tzdate('2000-01-01,America/Los_Angeles')
; -- same time value

SELECT
    RemoveTimezone(CAST('2000-01-01,GMT' AS tzdate)) == RemoveTimezone(tzdate('2000-01-01,America/Los_Angeles'))
;

SELECT
    CAST('2000-01-01T12:00:00,GMT' AS tzdatetime) > tzdatetime('2000-01-01T12:00:00,Europe/Moscow')
;

SELECT
    CAST('2000-01-01T09:00:00,GMT' AS tzdatetime) == tzdatetime('2000-01-01T12:00:00,Europe/Moscow')
;

SELECT
    RemoveTimezone(CAST('2000-01-01T09:00:00,GMT' AS tzdatetime)) == RemoveTimezone(tzdatetime('2000-01-01T12:00:00,Europe/Moscow'))
;

SELECT
    CAST('2000-01-01T12:00:00,GMT' AS tzdatetime) < tzdatetime('2000-01-01T12:00:00,America/Los_Angeles')
;

SELECT
    CAST('2000-01-01T20:00:00,GMT' AS tzdatetime) == tzdatetime('2000-01-01T12:00:00,America/Los_Angeles')
;

SELECT
    RemoveTimezone(CAST('2000-01-01T20:00:00,GMT' AS tzdatetime)) == RemoveTimezone(tzdatetime('2000-01-01T12:00:00,America/Los_Angeles'))
;

SELECT
    CAST('2000-01-01T12:00:00,GMT' AS tztimestamp) > tztimestamp('2000-01-01T12:00:00,Europe/Moscow')
;

SELECT
    CAST('2000-01-01T09:00:00,GMT' AS tztimestamp) == tztimestamp('2000-01-01T12:00:00,Europe/Moscow')
;

SELECT
    RemoveTimezone(CAST('2000-01-01T09:00:00,GMT' AS tztimestamp)) == RemoveTimezone(tztimestamp('2000-01-01T12:00:00,Europe/Moscow'))
;

SELECT
    CAST('2000-01-01T12:00:00,GMT' AS tztimestamp) < tztimestamp('2000-01-01T12:00:00,America/Los_Angeles')
;

SELECT
    CAST('2000-01-01T20:00:00,GMT' AS tztimestamp) == tztimestamp('2000-01-01T12:00:00,America/Los_Angeles')
;

SELECT
    RemoveTimezone(CAST('2000-01-01T20:00:00,GMT' AS tztimestamp)) == RemoveTimezone(tztimestamp('2000-01-01T12:00:00,America/Los_Angeles'))
;
