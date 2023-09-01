/* syntax version 0 */
$ts = COALESCE(DateTime::FromString(value), 0);
SELECT
   value,
   DateTime::GetSecond($ts) AS second,
   DateTime::GetMinute($ts) AS minute,
   DateTime::GetHour($ts) AS hour,
   DateTime::GetDayOfMonth($ts) AS day,
   DateTime::GetMonth($ts) AS month,
   DateTime::GetYear($ts) AS year,
   DateTime::IsWeekend($ts) AS is_weekend
FROM Input;
