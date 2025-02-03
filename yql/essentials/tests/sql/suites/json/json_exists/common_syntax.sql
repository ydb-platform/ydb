/* syntax version 1 */
/* postgres can not */

-- Null handling
SELECT
    JSON_EXISTS(NULL, "strict $.key"),
    JSON_EXISTS(Nothing(Json?), "strict $.key");

-- Casual select
$json = CAST(@@{"key": 128}@@ as Json);
SELECT
    JSON_EXISTS($json, "strict $.key");
