/* syntax version 1 */
$input = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/601.2.7 (KHTML, like Gecko) Version/9.0.1 Safari/601.2.7";
$capture = Re2::Capture(
    "(?:Mozilla|Opera)/(?P<major>\\d)\\.(?P<minor>\\d).*(Safari)"
);
$no_groups = Re2::Capture("(?:Intel) Mac");

SELECT
    $capture($input) AS capture,
    $no_groups($input) AS no_groups;
