/* syntax version 0 */
$ts = COALESCE(DateTime::FromString(value), 0);
SELECT
   value,
   DateTime::ToString(DateTime::FromTimeZone($ts, "Europe/Moscow")) AS absolute,
   DateTime::ToString(DateTime::ToTimeZone($ts, "Europe/Moscow")) AS civil
FROM Input;
