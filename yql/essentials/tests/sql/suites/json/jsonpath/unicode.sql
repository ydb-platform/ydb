/* syntax version 1 */
/* postgres can not */

-- Check access to members with unicode keys
$json = CAST(@@{
    "привет": 123
}@@ as Json);
SELECT
    JSON_EXISTS($json, @@strict $."привет"@@),
    JSON_VALUE($json, @@strict $."привет"@@);

$nested_json = CAST(@@{
    "привет": [1, 2, 3]
}@@ as Json);
SELECT
    JSON_QUERY($nested_json, @@strict $."привет"@@);