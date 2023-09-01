/* syntax version 0 */
$ts = cast(value as Date);
SELECT
   value,
   cast(DateTime::DateStartOfWeek($ts) as string),
   cast(DateTime::DatetimeStartOfWeek($ts) as string),
   cast(DateTime::TimestampStartOfWeek($ts) as string),
   cast(DateTime::DateStartOfMonth($ts) as string),
   cast(DateTime::DatetimeStartOfMonth($ts) as string),
   cast(DateTime::TimestampStartOfMonth($ts) as string),
   cast(DateTime::DateStartOfQuarter($ts) as string),
   cast(DateTime::DatetimeStartOfQuarter($ts) as string),
   cast(DateTime::TimestampStartOfQuarter($ts) as string),
   cast(DateTime::DateStartOfYear($ts) as string),
   cast(DateTime::DatetimeStartOfYear($ts) as string),
   cast(DateTime::TimestampStartOfYear($ts) as string)
FROM Input;
