/* syntax version 1 */
/* postgres can not */

-- Null handling
SELECT
    JSON_QUERY(NULL, "strict $.key"),
    JSON_QUERY(Nothing(Json?), "strict $.key");

-- Casual select
$json = CAST(@@{"key": [1, 2, 3]}@@ as Json);
SELECT
    JSON_QUERY($json, "strict $.key");
