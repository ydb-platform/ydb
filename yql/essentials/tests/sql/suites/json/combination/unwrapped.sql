/* syntax version 1 */
/* postgres can not */

$json = Unwrap(CAST(@@{"x": 1}@@ as Json));

SELECT
    JSON_EXISTS($json, "strict $.x"),
    JSON_VALUE($json, "strict $.x"),
    JSON_QUERY($json, "strict $");
