/* syntax version 1 */
/* postgres can not */

$json = JsonDocument(@@{"a": {"b": 1}, "c": 2}@@);

SELECT
    JSON_QUERY($json, "$.a"),
    JSON_QUERY($json, "$.b"),
    JSON_QUERY($json, "$.c"),
    JSON_QUERY(CAST(NULL as JsonDocument), "$.a");