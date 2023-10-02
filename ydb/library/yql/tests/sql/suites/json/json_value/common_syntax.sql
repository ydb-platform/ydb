/* syntax version 1 */
/* postgres can not */

-- Null handling
SELECT
    JSON_VALUE(NULL, "strict $.key"),
    JSON_VALUE(Nothing(Json?), "strict $.key");

-- Casual select
$json = CAST(@@{"key": 128}@@ as Json);
SELECT
    JSON_VALUE($json, "strict $.key");
