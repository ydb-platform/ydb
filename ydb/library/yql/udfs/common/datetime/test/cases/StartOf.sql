/* syntax version 0 */
$ts = COALESCE(CAST(value AS Uint64), 0);
SELECT
    DateTime::ToDate($ts) as Date,
    DateTime::ToDate(DateTime::StartOfWeek($ts)) as Week,
    DateTime::ToDate(DateTime::StartOfMonth($ts)) as Month,
    DateTime::ToDate(DateTime::StartOfQuarter($ts)) as Quarter,
    DateTime::ToDate(DateTime::StartOfYear($ts)) as Year
FROM Input;
