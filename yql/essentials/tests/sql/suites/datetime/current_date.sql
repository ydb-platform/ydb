/* postgres can not */
SELECT
    cast(CurrentUtcDate() as string) as `date`,
    cast(CurrentUtcDatetime() as string) as `datetime`,
    cast(CurrentUtcTimestamp() as string) as `timestamp`;
