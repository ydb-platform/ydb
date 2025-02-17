/* syntax version 1 */
/* postgres can not */

$json = JsonDocument(@@{"a": {"b": 1}, "c": 2}@@);

SELECT
    JSON_EXISTS($json, "$.a"),
    JSON_EXISTS($json, "$.b"),
    JSON_EXISTS($json, "$.c"),
    JSON_EXISTS(CAST(NULL as JsonDocument), "$.a");