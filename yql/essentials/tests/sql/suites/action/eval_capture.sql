/* syntax version 1 */
/* postgres can not */
$capture = Re2::Capture(".*" || CAST(Unicode::ToLower("(\\D+)"u) AS String) || ".*");
SELECT $capture(" 123 ");
