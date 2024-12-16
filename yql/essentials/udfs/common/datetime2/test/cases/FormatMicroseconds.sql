/* syntax version 1 */
$parse = DateTime::Parse("%Y-%m-%d %H:%M:%S");

$dt0 = $parse("2024-01-01 00:00:00");
$dt1 = $parse("2024-01-01 00:00:00.000001");
$dt2 = $parse("2024-01-01 00:00:00.05");

$format = DateTime::Format("%Y-%m-%d %H:%M:%S");
$format_ms = DateTime::Format("%Y-%m-%d %H:%M:%S", True as AlwaysWriteFractionalSeconds);

SELECT 
    $format($dt0), $format_ms($dt0),
    $format($dt1), $format_ms($dt1),
    $format($dt2), $format_ms($dt2)
;
