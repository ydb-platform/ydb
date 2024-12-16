/* syntax version 1 */
/* postgres can not */

$json = JsonDocument(@@{"a": {"b": 1}, "c": 2}@@);

SELECT
    JSON_VALUE($json, "$.c"),
    JSON_VALUE($json, "$.b"),
    JSON_VALUE($json, "$.a"),
    JSON_VALUE(CAST(NULL as JsonDocument), "$.a");