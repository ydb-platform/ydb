$rows = [
    <|fdatetime: "2025-09-01T15:02:47 with +0100 offset", fnginx: "01/Sep/2025:15:02:47 +0100"|>,
    <|fdatetime: "2025-09-01T15:02:47 with +02:00 offset", fnginx: "01/Sep/2025:15:02:47 +02:00"|>,
    <|fdatetime: "2025-09-01T15:02:47 with +03: offset", fnginx: "01/Sep/2025:15:02:47 +03:"|>,
    <|fdatetime: "2025-09-01T15:02:47 with +04 offset", fnginx: "01/Sep/2025:15:02:47 +04"|>,
    <|fdatetime: "2025-09-01T15:02:47 with +05:0 offset", fnginx: "01/Sep/2025:15:02:47 +05:0"|>,
    <|fdatetime: "2025-09-01T15:02:47 with +6:00 offset", fnginx: "01/Sep/2025:15:02:47 +6:00"|>,
    <|fdatetime: "2025-09-01T15:02:47 with 07:00 offset", fnginx: "01/Sep/2025:15:02:47 07:00"|>,
    <|fdatetime: "2025-09-01T15:02:47 with +8 offset", fnginx: "01/Sep/2025:15:02:47 +8"|>,
    <|fdatetime: "2025-09-01T15:02:47 with -09:00 offset", fnginx: "01/Sep/2025:15:02:47 -09:00"|>,
    <|fdatetime: "2025-09-01T15:02:47 with -00:00 offset", fnginx: "01/Sep/2025:15:02:47 -00:00"|>,
];

$parseNginx = DateTime::Parse64("%d/%b/%Y:%H:%M:%S %z");
$parseTzOffset = DateTime::Parse64("%Y-%m-%dT%H:%M:%S with %z offset");

$format = DateTime::Format("%Y-%m-%dT%H:%M:%S%z(%Z)");

select
    $format($parseNginx(fnginx)) as onginx,
    $format($parseTzOffset(fdatetime)) as odatetime,
FROM AS_TABLE($rows);
