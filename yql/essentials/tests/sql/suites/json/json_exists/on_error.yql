/* syntax version 1 */
/* postgres can not */

-- Accessing absent object member will cause jsonpath error in strict mode
$json = CAST("{}" as Json);
SELECT
    JSON_EXISTS($json, "strict $.key"), -- defaults to FALSE ON ERROR
    JSON_EXISTS($json, "strict $.key" FALSE ON ERROR),
    JSON_EXISTS($json, "strict $.key" TRUE ON ERROR),
    JSON_EXISTS($json, "strict $.key" UNKNOWN ON ERROR);