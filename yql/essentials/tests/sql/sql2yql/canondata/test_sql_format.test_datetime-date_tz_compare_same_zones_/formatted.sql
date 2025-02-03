/* postgres can not */
SELECT
    CAST('2000-01-01,GMT' AS tzdate) == tzdate('2000-01-01,GMT')
;

SELECT
    CAST('2000-01-01,GMT' AS tzdate) < CAST('2000-01-01' AS date)
;

SELECT
    CAST('2000-01-01,GMT' AS tzdate) <= tzdate('2000-01-01,GMT')
;

SELECT
    CAST('2000-01-01,GMT' AS tzdate) > tzdate('2000-01-01,GMT')
;

SELECT
    CAST('2000-01-01,GMT' AS tzdate) >= tzdate('2000-01-01,GMT')
;

SELECT
    CAST('2000-01-01,GMT' AS tzdate) != tzdate('2000-01-01,GMT')
;

SELECT
    tzdate('2000-01-01,GMT') == CAST('2000-01-01,GMT' AS tzdate)
;

SELECT
    tzdate('2000-01-01,GMT') < CAST('2000-01-01,GMT' AS tzdate)
;

SELECT
    tzdate('2000-01-01,GMT') <= CAST('2000-01-01,GMT' AS tzdate)
;

SELECT
    tzdate('2000-01-01,GMT') > CAST('2000-01-01,GMT' AS tzdate)
;

SELECT
    tzdate('2000-01-01,GMT') >= CAST('2000-01-01,GMT' AS tzdate)
;

SELECT
    tzdate('2000-01-01,GMT') != CAST('2000-01-01,GMT' AS tzdate)
;

SELECT
    CAST('2000-01-01T12:00:00,GMT' AS tzdatetime) == tzdatetime('2000-01-01T12:00:00,GMT')
;

SELECT
    CAST('2000-01-01T12:00:00,GMT' AS tzdatetime) < tzdatetime('2000-01-01T12:00:00,GMT')
;

SELECT
    CAST('2000-01-01T12:00:00,GMT' AS tzdatetime) <= tzdatetime('2000-01-01T12:00:00,GMT')
;

SELECT
    CAST('2000-01-01T12:00:00,GMT' AS tzdatetime) > tzdatetime('2000-01-01T12:00:00,GMT')
;

SELECT
    CAST('2000-01-01T12:00:00,GMT' AS tzdatetime) >= tzdatetime('2000-01-01T12:00:00,GMT')
;

SELECT
    CAST('2000-01-01T12:00:00,GMT' AS tzdatetime) != tzdatetime('2000-01-01T12:00:00,GMT')
;

SELECT
    tzdatetime('2000-01-01T12:00:00,GMT') == CAST('2000-01-01T12:00:00,GMT' AS tzdatetime)
;

SELECT
    tzdatetime('2000-01-01T12:00:00,GMT') < CAST('2000-01-01T12:00:00,GMT' AS tzdatetime)
;

SELECT
    tzdatetime('2000-01-01T12:00:00,GMT') <= CAST('2000-01-01T12:00:00,GMT' AS tzdatetime)
;

SELECT
    tzdatetime('2000-01-01T12:00:00,GMT') > CAST('2000-01-01T12:00:00,GMT' AS tzdatetime)
;

SELECT
    tzdatetime('2000-01-01T12:00:00,GMT') >= CAST('2000-01-01T12:00:00,GMT' AS tzdatetime)
;

SELECT
    tzdatetime('2000-01-01T12:00:00,GMT') != CAST('2000-01-01T12:00:00,GMT' AS tzdatetime)
;

SELECT
    CAST('2000-01-01T12:00:00.123456,GMT' AS tztimestamp) == tztimestamp('2000-01-01T12:00:00.123456,GMT')
;

SELECT
    CAST('2000-01-01T12:00:00.123456,GMT' AS tztimestamp) < tztimestamp('2000-01-01T12:00:00.123456,GMT')
;

SELECT
    CAST('2000-01-01T12:00:00.123456,GMT' AS tztimestamp) <= tztimestamp('2000-01-01T12:00:00.123456,GMT')
;

SELECT
    CAST('2000-01-01T12:00:00.123456,GMT' AS tztimestamp) > tztimestamp('2000-01-01T12:00:00.123456,GMT')
;

SELECT
    CAST('2000-01-01T12:00:00.123456,GMT' AS tztimestamp) >= tztimestamp('2000-01-01T12:00:00.123456,GMT')
;

SELECT
    CAST('2000-01-01T12:00:00.123456,GMT' AS tztimestamp) != tztimestamp('2000-01-01T12:00:00.123456,GMT')
;

SELECT
    tztimestamp('2000-01-01T12:00:00.123456,GMT') == CAST('2000-01-01T12:00:00.123456,GMT' AS tztimestamp)
;

SELECT
    tztimestamp('2000-01-01T12:00:00.123456,GMT') < CAST('2000-01-01T12:00:00.123456,GMT' AS tztimestamp)
;

SELECT
    tztimestamp('2000-01-01T12:00:00.123456,GMT') <= CAST('2000-01-01T12:00:00.123456,GMT' AS tztimestamp)
;

SELECT
    tztimestamp('2000-01-01T12:00:00.123456,GMT') > CAST('2000-01-01T12:00:00.123456,GMT' AS tztimestamp)
;

SELECT
    tztimestamp('2000-01-01T12:00:00.123456,GMT') >= CAST('2000-01-01T12:00:00.123456,GMT' AS tztimestamp)
;

SELECT
    tztimestamp('2000-01-01T12:00:00.123456,GMT') != CAST('2000-01-01T12:00:00.123456,GMT' AS tztimestamp)
;
