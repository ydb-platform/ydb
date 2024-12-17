/* postgres can not */
SELECT
    1 / 0 IS NULL,
    1 / 0 ISNULL,
    1 / 0 IS NOT NULL,
    1 / 0 NOTNULL
;
