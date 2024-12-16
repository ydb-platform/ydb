/* syntax version 1 */
/* postgres can not */

-- Accessing absent object member will cause jsonpath error in strict mode
$json = CAST("{}" as Json);
SELECT
    JSON_QUERY($json, "strict $.key"), -- defaults to NULL ON ERROR
    JSON_QUERY($json, "strict $.key" NULL ON ERROR),
    JSON_QUERY($json, "strict $.key" EMPTY ARRAY ON ERROR),
    JSON_QUERY($json, "strict $.key" EMPTY OBJECT ON ERROR);