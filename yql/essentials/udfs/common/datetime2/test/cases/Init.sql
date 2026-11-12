$defaults = DateTime::Init();
$nulls = DateTime::Init(NULL as Year, NULL as Month, NULL as Day, NULL as Hour, NULL as Minute, NULL as Second, NULL as Microsecond);
$custom = DateTime::Init(2024 as Year, 2 as Month, 29 as Day, 12 as Hour, 34 as Minute, 56 as Second, 123456 as Microsecond, "Europe/Moscow" as Timezone);
$invalidTimezone = DateTime::Init(2000 as Year, "Eurpe/Moscow" as Timezone);
$invalid = DateTime::Init(2023 as Year, 2 as Month, 29 as Day);

SELECT
    <|year: DateTime::GetYear($defaults), month: DateTime::GetMonth($defaults), day: DateTime::GetDayOfMonth($defaults), hour: DateTime::GetHour($defaults), minute: DateTime::GetMinute($defaults), second: DateTime::GetSecond($defaults), microsecond: DateTime::GetMicrosecondOfSecond($defaults), timezone_id: DateTime::GetTimezoneId($defaults)|> AS defaults,
    <|year: DateTime::GetYear($nulls), month: DateTime::GetMonth($nulls), day: DateTime::GetDayOfMonth($nulls), hour: DateTime::GetHour($nulls), minute: DateTime::GetMinute($nulls), second: DateTime::GetSecond($nulls), microsecond: DateTime::GetMicrosecondOfSecond($nulls), timezone_id: DateTime::GetTimezoneId($nulls)|> AS nulls,
    <|year: DateTime::GetYear($custom), month: DateTime::GetMonth($custom), day: DateTime::GetDayOfMonth($custom), hour: DateTime::GetHour($custom), minute: DateTime::GetMinute($custom), second: DateTime::GetSecond($custom), microsecond: DateTime::GetMicrosecondOfSecond($custom), timezone: DateTime::GetTimezoneName($custom)|> AS custom,
    DateTime::GetYear($invalidTimezone) AS invalid_timezone,
    CAST(DateTime::MakeTimestamp($invalid) AS String) AS invalid;
