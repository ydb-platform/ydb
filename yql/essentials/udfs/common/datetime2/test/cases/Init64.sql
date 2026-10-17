$defaults = DateTime::Init64();
$custom = DateTime::Init64(148108 as Year, 2 as Month, 29 as Day, 12 as Hour, 34 as Minute, 56 as Second, 123456 as Microsecond, "Europe/Moscow" as Timezone);
$negativeYear = DateTime::Init64(-1 as Year);
$year1900 = DateTime::Init64(1900 as Year);
$invalid = DateTime::Init64(2023 as Year, 2 as Month, 29 as Day);

SELECT
    <|year: DateTime::GetYear($defaults), month: DateTime::GetMonth($defaults), day: DateTime::GetDayOfMonth($defaults), hour: DateTime::GetHour($defaults), timezone_id: DateTime::GetTimezoneId($defaults)|> AS defaults,
    <|year: DateTime::GetYear($custom), month: DateTime::GetMonth($custom), day: DateTime::GetDayOfMonth($custom), hour: DateTime::GetHour($custom), minute: DateTime::GetMinute($custom), second: DateTime::GetSecond($custom), microsecond: DateTime::GetMicrosecondOfSecond($custom), timezone: DateTime::GetTimezoneName($custom)|> AS custom,
    DateTime::GetYear($negativeYear) AS negative_year,
    DateTime::GetYear($year1900) AS year_1900,
    DateTime::GetYear($invalid) AS invalid;
