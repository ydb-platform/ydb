/* syntax version 1 */
$parse = DateTime::Parse("%Y-%m-%d %H:%M:%S");
$dt = $parse("2025-12-04 23:24:54");

$dt_utc = DateTime::Update($dt, 'UTC' as Timezone);
$dt_gmt = DateTime::Update($dt, 'GMT' as Timezone);
$dt_lnd = DateTime::Update($dt, 'Europe/London' as Timezone);
$dt_msk = DateTime::Update($dt, 'Europe/Moscow' as Timezone);
$dt_los = DateTime::Update($dt, 'America/Los_Angeles' as Timezone);

$format = DateTime::Format("%Y-%m-%d %H:%M:%S%z");
$format_c = DateTime::Format("%Y-%m-%d %H:%M:%S%z", True as WriteOffsetWithColon);

SELECT
    $format($dt_utc) as utc0, $format_c($dt_utc) as utcZ,
    $format($dt_gmt) as gmt0, $format_c($dt_gmt) as gmtZ,
    $format($dt_lnd) as lnd0, $format_c($dt_lnd) as lndZ,
    $format($dt_msk) as msk0, $format_c($dt_msk) as mskZ,
    $format($dt_los) as los0, $format_c($dt_los) as losZ
;
