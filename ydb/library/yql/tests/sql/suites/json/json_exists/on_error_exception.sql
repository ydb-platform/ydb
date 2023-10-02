/* syntax version 1 */
/* postgres can not */

-- Accessing absent object member will cause jsonpath error in strict mode
$json = CAST("{}" as Json);
SELECT
    JSON_EXISTS($json, "strict $.key" ERROR ON ERROR);