/* postgres can not */
SELECT
    CAST(CurrentUtcDate() AS string) AS `date`,
    CAST(CurrentUtcDatetime() AS string) AS `datetime`,
    CAST(CurrentUtcTimestamp() AS string) AS `timestamp`
;
